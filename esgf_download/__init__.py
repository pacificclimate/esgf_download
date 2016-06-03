'''
The esgf_download module includes classes for both retrieving metadata and
downloading data. It makes use of the ESGF JSON query interface to determine
which servers and data sets to query, then queries the XML for each data set
from each of the THREDDS servers.
'''

from datetime import datetime
import logging
import time
import pdb
import requests
import urllib2
import threading
import os
import signal
import errno
import sys
import sqlite3
from collections import deque
import Queue
import pyesgf
from pyesgf.search import SearchConnection
from pyesgf.logon import LogonManager
import re

from lxml import etree
from pkg_resources import resource_stream

from writer import MultiFileWriter
from downloader import DownloadThread

log = logging.getLogger(__name__)

def get_request(requests_object, url, **kwargs):
    '''
    Function which performs an HTTP GET request with a session object.

    This function performs an HTTP GET request with a supplied session object,
    returning application-appropriate exceptions where applicable. This allows
    for download of data with authentication.

    Example::
     from esgf_download import get_request, make_session
     session = make_session();
     get_request(session, "http://some/url/")

    :param requests_object: The session object to use.
    :param url: The URL to retrieve.
    :param **kwargs: Parameters to be passed on to Requests.get
    :rtype: Response object.
    
    '''
    try:
        fetch_request = requests_object.get(url, **kwargs)
    except requests.RequestException as e:
        raise Exception("REQUESTS_UNKNOWN_ERROR: " + str(e))
    except requests.ConnectionError as e:
        raise Exception(request_error = "CONNECTION_ERROR: " + str(e))
    except requests.HTTPError as e:
        raise Exception("HTTP_ERROR: " + str(e))
    except requests.URLRequired as e:
        raise Exception("NOURL_ERROR")
    except requests.TooManyRedirects as e:
        raise Exception("TOO_MANY_REDIRECTS")
    except error as e:
        raise Exception("UNKNOWN_ERROR: " + str(e))

    # HTTP error handling
    if(fetch_request.status_code != 200):
        response_dict = {403: "AUTH_FAIL", 404: "FILE_NOT_FOUND", 500: "SERVER_ERROR" }
        if fetch_request.status_code in response_dict:
            raise Exception(response_dict[fetch_request.status_code])
        else:
            raise Exception(str(fetch_request.status_code))

    return fetch_request

class Host:
    '''
    Describes a host's parameters (maximum threads, data node).
    '''
    def __init__(self, max_thread_count, datanode):
        '''
        Creates a Host object.
        :param max_thread_count: The maximum number of download threads to use for this host.
        :param datanode: The base URL for the data node.
        '''
        self.max_thread_count = max_thread_count
        self.datanode = datanode
        self.thread_count = 0
        self.session = make_session()
        self.download_queue = deque()

class Downloader:
    '''
    A downloader which downloads files as specified in the database file,
    with the authentication credentials provided.
    '''
    def __init__(self,
                 database_file,
                 base_path,
                 username,
                 password,
                 auth_server,
                 initial_threads_per_host=3,
                 max_total_threads=100,
                 **kwargs):
        '''
        Creates a Downloader object.
        :param database_file: Sqlite3 database file where information
            is stored on files to be downloaded.
        :param base_path: Base path to store downloaded files in.
        :param username: Username to use for authentication.
        :param password: Password to use for authentication.
        :param auth_server: Authentication server to use to authenticate.
        :param initial_threads_per_host: Initial number of threads per host.
        :param max_total_threads: Maximum number of independent downloads.
        '''
        self.base_path = base_path
        self.username = username
        self.password = password
        self.auth_server = auth_server
        self.max_queue_len = max_total_threads * 2
        self.initial_threads_per_host = initial_threads_per_host
        self.max_total_threads = max_total_threads
        self.total_threads = 0

        # Database jazz. 2 connections due to Python limitations; lock due to not using WAL yet.
        self.conn = sqlite3.connect(database_file)
        self.database_lock = threading.Lock()
        self.database_file = database_file

        # Queues for incoming metadata and events
        self.event_queue = Queue.Queue()
        self.metadata_queue = Queue.Queue()

        # Queues per model, and collections of threads.
        self.download_threads = {}
        self.hosts = {}

    def metadata_reader(self):
        '''
        Routine which retrieves metadata and places it into a queue to be processed.
        Spawned as a thread. Internal.
        '''
        log.debug("Starting metadata reader...")
        last_transfert_id = 0
        reader_conn = sqlite3.connect(self.database_file)
        reader_conn.row_factory = sqlite3.Row
        curse = reader_conn.cursor()

        # FIXME: The method used here is kind of wrong. It should check for anything that's
        # changed; but instead it only checks for stuff that's new.
        while self.running:
            try:
                with self.database_lock:
                    for row in curse.execute("SELECT transfert.*,model.* " +
                        "FROM transfert JOIN model ON model.name=transfert.model " +
                        "WHERE status = 'waiting' AND transfert_id > ?", [last_transfert_id]):
                        self.metadata_queue.put(row)
                        last_transfert_id = max(row['transfert_id'], last_transfert_id)
            except sqlite3.Error as se:
                log.error("Error querying for new transfers; shutting down.")
                self.running = False
                continue
            time.sleep(60)
        log.debug("Metadata reader exiting...")

    def handle_events(self):
        '''
        Routine which appropriately dequeues and handles events passed back from download threads. Internal.
        '''
        while not self.event_queue.empty():
            try:
                ev, transfert_id, data = self.event_queue.get(timeout=5)
            except Exception as e:
                continue
            thread = self.download_threads[transfert_id]
            update_fields = None

            if ev == "ERROR":
                # TODO: Add more appropriate error handling
                # Specifically, something that behaves differently depending on the error message
                # so that we can realize when a connection's been reset, etc, and can respond
                # appropriately by scaling back # threads.
                log.warning("Error downloading " + thread.url + ": " + data)
                update_fields = { 'status': 'error', 'error_msg': data }
            elif ev == "LENGTH":
                update_fields = { 'status': 'running' }
                thread.length = data
            elif ev == "SPEED":
                log.debug("ID: " + str(transfert_id) + ", Speed: " + str(data) + "kb/s")
            elif ev == "ABORTED":
                log.error("Download aborted: " + thread.filename + ", Reason: " + data)
                update_fields = { 'status': 'waiting' }
            elif ev == "DONE":
                log.info("Finished downloading " + thread.filename)
                update_fields = { 'status': 'done' }
        
            if update_fields is not None:
                if update_fields['status'] != 'running':
                    update_fields['duration'] = thread.end_time - thread.start_time
                    update_fields['rate'] = thread.data_size / update_fields['duration']
                    update_fields['start_date'] = thread.start_time
                    update_fields['end_date'] = thread.end_time
                    thread.download_thread.join()
                    self.hosts[thread.host].thread_count -= 1
                    self.total_threads -= 1
                    del self.download_threads[transfert_id]
                with self.database_lock:
                    try:
                        self.conn.execute(
                            'UPDATE transfert ' +
                            'SET ' + ",".join([ x + " = ?" for x in update_fields.keys() ]) +
                            ' WHERE transfert_id = ?', update_fields.values() + [transfert_id])
                        self.conn.commit()
                    except sqlite3.Error as se:
                        if not self.stop_now:
                            log.error("Error updating transfert table; shutting down." +
                                "Do you have write permissions to the database?")
                            self.shutdown_now(None, None)
                            break

    # TODO: Make this do something.
    def adjust_hosts_max_thread_count(self):
        '''
        Adjusts max thread count based on feedback. Right now, this does nothing.
        '''
        pass
    
    def auth(self):
        '''
        Authenticate with the auth server specified on object creation.
        '''
        # Check that we're logged on
        lm = LogonManager()
        log.debug('Logon manager started')
        if not lm.is_logged_on():
            log.debug(self.username, self.password, self.auth_server)
            lm.logon(self.username, self.password, self.auth_server)
        if not lm.is_logged_on():
            raise Exception('NOAUTH')

    def shutdown_now(self, signum, frame):
        '''
        Sets flags to specify that shutdown should happen immediately. Wired up as a signal handler.
        '''
        self.running = False
        self.stop_now = True
        
    def go_get_em(self):
        '''
        Routine which spawns the MultiFileWriter, performs authentication, spawns the
        MetadataReader, and begins downloading data.
        '''
        # Set stuff to run.
        self.running = True
        self.stop_now = False

        # Set up signal handler
        signal.signal(signal.SIGTERM, self.shutdown_now)

        try:
            self.auth()
        except:
            log.error("Couldn't log on using the provided credentials; exiting.")
            return

        # Write serializer thread
        writer = MultiFileWriter(self.max_queue_len)

        # Metadata reader thread; feeds this thread.
        md_reader_thread = threading.Thread(target=self.metadata_reader, name="MetadataReaderThread")
        md_reader_thread.daemon = True
        md_reader_thread.start()

        # Then, for each model, queue up to n jobs.
        # The jobs communicate back to the parent thread here and statistics are gathered.
        while self.running:
            try:
                # TODO: Split into function
                while not self.metadata_queue.empty():
                    item = self.metadata_queue.get(timeout=5)
                    if(item['datanode'] not in self.hosts):
                        self.hosts[item['datanode']] = Host(self.initial_threads_per_host, item['datanode'])
                    self.hosts[item['datanode']].download_queue.append(item)

                # Queue up threads to run from host queues
                for hostname, host in self.hosts.items():
                    while ((len(host.download_queue) != 0)
                            and host.thread_count < host.max_thread_count
                            and self.total_threads < self.max_total_threads):
                        item = host.download_queue.popleft()
                        self.download_threads[item['transfert_id']] = DownloadThread(
                            item['location'],
                            item['datanode'],
                            item['transfert_id'],
                            self.base_path + "/" + item['local_image'],
                            item['checksum'],
                            item['checksum_type'],
                            writer,
                            self.event_queue,
                            host.session)

                        host.thread_count += 1
                        self.total_threads += 1
                        self.handle_events()
                        time.sleep(0.2)

                self.adjust_hosts_max_thread_count()

                self.handle_events()
                time.sleep(0.1)
            except KeyboardInterrupt:
                self.shutdown_now(None, None)

        # If we're stopping _NOW_, update the database and nuke the files, then shut down.
        if self.stop_now:
            log.info("Shutting threads down right now...")
            writer.write_and_quit()
            for dt in self.download_threads.values():
                with dt.abort_lock:
                    dt.abort = True
                self.conn.execute(
                    "UPDATE transfert " +
                    "SET status='waiting' " +
                    "WHERE transfert_id = ?", [dt.transfert_id])
            self.conn.commit()
            log.debug("Waiting 10s in the hopes threads die...")
            time.sleep(10)
            for dt in self.download_threads.values():
                try:
                    os.unlink(dt.filename)
                except Exception as e:
                    pass
        else:
            log.info("Waiting for remaining threads to finish...")
            while self.total_threads > 0:
                self.handle_events()
                time.sleep(0.2)
            log.info("All download threads have shut down.")
            writer.write_and_quit()
        time.sleep(1)
        log.info("Writer thread has shut down. Have a nice day!")
        
def make_session():
    '''
    Creates a session, assuming the session certificate will be stored in $HOME/.esg/credentials.pem .
    '''
    sesh = requests.Session()
    sesh.cert = os.environ['HOME'] + '/.esg/credentials.pem'
    sesh.max_redirects = 5
    sesh.stream = True
    sesh.verify = False
    return sesh

def unlist(x):
    '''
    Takes an object, returns the 1st element if it is a list, thereby removing list wrappers from singletons.
    '''
    if isinstance(x, list):
        return x[0]
    else:
        return x

def get_property_dict(xml_tree,
                      xpath_text='ud:property',
                      namespaces={'ud':'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}):
    '''
    Performs the given xpath query on the given xml_tree,
    with the given namespace, returning a dictionary.

    :param xml_tree: The XML element tree to operate on.
    :param xpath_text: The XPath query to use.
    :param namespaces: The namespace to use for the document.
    :rtype: Dictionary of name to value.
    '''
    return {x.get('name'):x.get('value') for x in xml_tree.xpath(xpath_text, namespaces=namespaces)}

# Constraints can be lists of values, but must be named.
# TODO: Fetch multiple XML files at once.
# Try using select()?
def metadata_update(database_file,
                    search_host="http://pcmdi11.llnl.gov/esg-search/search",
                    **constraints):
    '''
    Queries the ESGF server for a set of datasets, queries each THREDDS
    server for metadata for each data set (the list of files), and records
    information about datasets and data files in the given database file.

    :param database_file: The database file to store information in.
    :param search_host: The search host to use.
    :param **constraints: The constraints for the search.
    '''

    db_exists = os.path.isfile(database_file)

    conn = sqlite3.connect(database_file)

    ## Stick the schema in the database if it is absent.
    if not db_exists:
        schema_text = resource_stream('esgf_download', '/data/schema.sql')
        for line in schema_text:
            conn.execute(line)
        conn.commit()

    curse = conn.cursor()

    # Turn on debugging, for great justice.
    logging.basicConfig(level='DEBUG', stream=sys.stdout)
    
    search_conn = SearchConnection(search_host, distrib=True)
    ## Need to turn on WAL: http://www.sqlite.org/draft/wal.html
    ctx = pyesgf.search.SearchContext(search_conn, constraints, replica=False, search_type=pyesgf.search.TYPE_DATASET)

    field_map_model = {
        'data_node': 'datanode',
        'institute': 'institute',
        'model': 'name' }
    field_map_transfert = {
        'model': 'model',
        'checksum': 'checksum',
        'size': 'fsize',
        'variable': 'variable',
        'tracking_id': 'tracking_id',
        'version': 'version_xml_tag',
        'size': 'size_xml_tag',
        'checksum_type': 'checksum_type',
        'product': 'product_xml_tag',
        'product': 'local_product',
        'local_image': 'local_image',
        'status': 'status',
        'location': 'location'}

    model_fetch_query = "SELECT name from model where name = ?"
    model_insert_query = "INSERT INTO model({}) VALUES({}})".format(
        ",".join(field_map_model.values()),
        ",".join(["?"] * len(field_map_model))
    )
    transfert_fetch_query = "SELECT transfert_id from transfert where tracking_id = ?"
    transfert_insert_query = "INSERT INTO transfert({}) VALUES({})".format(
        ",".join(field_map_transfert.values()),
        ",".join(["?"] * len(field_map_transfert))
    )


    output_path_json_bits = [
        'project',
        'product',
        'institute',
        'clean_model',
        'experiment',
        'time_frequency',
        'realm',
        'cmor_table',
        'ensemble',
        'version',
        'variable',
        'filename']

    ns = {'ud':'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}
    get_master_dataset = etree.XPath("/ud:catalog/ud:dataset", namespaces=ns)
    get_thredds_server_base = etree.XPath(
        "/ud:catalog/ud:service[@name='fileservice' or @name='fileService']" +
        "/ud:service[@name='HTTPServer' or @serviceType='HTTPServer']", namespaces=ns)
    get_thredds_server_base_alt = etree.XPath(
        "ud:service[@name='HTTPServer' or @serviceType='HTTPServer']", namespaces=ns)
    get_variables = etree.XPath("ud:variables/ud:variable", namespaces=ns)

    ds = ctx.search()
    for ds0 in ds:
        ## TODO: REFINE THIS: Parse the date coded version out of the URL and compare it to the most recent version in the database. If it's newer, index it. Otherwise, don't. This will save a lot of time.
        try:
            xml_query = get_request(requests, unlist(ds0.json['url']))
        except Exception as e:
            log.warning('Error fetching metadata from ' + unlist(ds0.json['url']) + ': ' + str(e))
            continue

        tree = etree.XML(xml_query.content)
        log.debug("Fetched metadata from thredds server...")

        dataset_metadata = get_property_dict(get_master_dataset(tree)[0])
        httpserver = get_thredds_server_base(tree)
        if len(httpserver) == 0:
            httpserver = get_thredds_server_base_alt(tree)
            if len(httpserver) == 0:
                log.warning("Could not find a base for the Thredds HTTP server; not considering this data.")
                continue

        thredds_server_base = httpserver[0].get('base')
        thredds_httpserver_service_name = httpserver[0].get('name')

        # Check whether model in table; if not, add it.
        curse.execute(model_fetch_query, [unlist(ds0.json["model"])])
        num_results = len(curse.fetchall())
        if(num_results == 0):
            conn.execute(model_insert_query, [unlist(ds0.json[x]) for x in field_map_model.keys()])
            conn.commit()

        ## Winnow away the variables we don't want and loop over the remainder
        filter_elements = etree.XPath("/ud:catalog/ud:dataset/ud:dataset[ud:serviceName='" +
            thredds_httpserver_service_name + "']/ud:variables/ud:variable[" +
            " or ".join(["@name='%s'" % var for var in constraints['variable'] ]) +
            "]/../..", namespaces=ns)
        matches = filter_elements(tree)
        for ds_file in matches:
            file_metadata = get_property_dict(ds_file)
            metadata = dict(ds0.json, **file_metadata)

            # Get details that shouild be included in metadata and put them in there.
            metadata['version'] = datetime.strptime(metadata["mod_time"], "%Y-%m-%d %H:%M:%S").strftime("v%Y%m%d")
            metadata['filename'] = ds_file.get('name')
            # FIXME: Check for >0 vars
            metadata['variable'] = get_variables(ds_file)[0].get('name')
            metadata['clean_model'] = re.split("_", metadata['filename'])[2]
            metadata['local_image'] = "/".join([ unlist(metadata[x]) for x in output_path_json_bits ])
            metadata['location'] = "http://" + metadata['data_node'] + thredds_server_base + ds_file.get('urlPath')
            metadata['status'] = 'waiting'

            curse.execute(transfert_fetch_query, [unlist(metadata['tracking_id'])])
            num_results = len(curse.fetchall())
            if(num_results == 0):
                # Check that all the bits that should be there, are.
                missing_keys = field_map_transfert.viewkeys() - metadata.viewkeys()
                if len(missing_keys) > 0:
                    log.warning("Error: dataset object " +
                        metadata['location'] +
                        " will be omitted as it is missing the following keys: " +
                        ",".join(missing_keys))
                    continue
                conn.execute(transfert_insert_query, [unlist(metadata[x]) for x in field_map_transfert.keys()])
                conn.commit()
                log.debug("Inserted a transfer...")
