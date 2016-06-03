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
import sys
import sqlite3
import pyesgf
from pyesgf.search import SearchConnection
import re

from lxml import etree
from pkg_resources import resource_stream

from downloader import Downloader

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
    model_insert_query = "INSERT INTO model({}) VALUES({})".format(
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
