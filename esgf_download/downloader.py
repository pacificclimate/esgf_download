import os
import errno
import time
import threading
import logging
import sqlite3
import Queue
import signal
import md5 ## FIXME: Use hashlib

from pyesgf.logon import LogonManager

from writer import MultiFileWriter
from host import Host

log = logging.getLogger(__name__)

class DownloadThread:
    '''
    A downloader which, upon creation, starts up a thread which downloads the
    specified data, allowing the spawning process to continue its business.
    Checks for errors and reports them in the event_queue.
    '''
    def __init__(self,
                 url,
                 host,
                 transfert_id,
                 filename,
                 checksum,
                 checksum_type,
                 writer,
                 event_queue,
                 session):
        '''
        Creates a DownloadThread and starts it.
        :param url: URL to download.
        :param host: Host to download from.
        :param transfert_id: Database ID for this transfer.
        :param filename: Filename to write out to.
        :param checksum: Checksum that the file should have when file is downloaded.
        :param writer: MultiFileWriter object which serializes writing.
        :param event_queue: A Queue to put events (failures to download,
            successes, corruption) in.
        :param session: The Requests session object to be used for auth.
        '''
        ## Possibly use **kwargs + self.__dict assignment + self.__dict.update()
        self.checksum = checksum
        self.checksum_type = checksum_type
        self.url = url
        self.host = host
        self.transfert_id = transfert_id
        self.filename = filename
        self.writer = writer
        self.event_queue = event_queue
        self.session = session
        self.data_size = 0
        self.perf_list = []
        self.num_recs = 5
        self.abort_lock = threading.Lock()
        self.abort = False
        self.blocksize = 1024 * 1024
        self.download_thread = threading.Thread(target=self.download, name=filename)
        self.download_thread.daemon = True
        self.download_thread.start()

    def mark_start_time(self):
        '''
        Records the start time for the download. Internal.
        '''
        self.start_time = time.time()

    def mark_end_time(self):
        '''
        Records the end time for the download. Internal.
        '''
        self.end_time = time.time()

    def add_perf_num(self, kbps):
        '''
        Add a record to the running mean download speed record. Internal.
        :param kbps: The download speed for the last interval in kbps.
        '''
        self.perf_list.append(kbps)
        if(len(self.perf_list) > self.num_recs):
            self.perf_list.pop(0)
        return

    def get_avg_perf(self):
        '''
        Get the average download speed over the last n intervals.
        :rtype: Number representing speed in kbps.
        '''
        avg_perf = 0
        for item in self.perf_list:
            avg_perf += item
        return avg_perf / len(self.perf_list)

    def download(self):
        '''
        Routine which comprises the main download task. Spawned as a thread. Internal.
        '''
        log.info("Initializing download of " + self.filename)
        self.mark_start_time()

        if not (self.checksum_type == "MD5" or self.checksum_type == "md5"):
            self.event_queue.put(("ERROR", self.transfert_id, "UNSUPPORTED_CHECKSUM_TYPE"))
        data_md5sum = md5.new()

        request_error = None
        try:
            res = get_request(self.session, self.url, stream=True)
        except Exception as e:
            self.mark_end_time()
            self.event_queue.put(("ERROR", self.transfert_id, str(e)))
            return
        
        self.event_queue.put(("LENGTH", self.transfert_id, res.headers['content-length']))

        # Download data
        # TODO: Global exception handling
        try:
            try:
                os.makedirs(os.path.dirname(self.filename))
            except os.error as e:
                if e.errno != errno.EEXIST:
                    raise
            with self.abort_lock:
                if not self.abort:
                    fd = open(self.filename, "wb+")
        except os.error as e: 
            self.mark_end_time()
            self.event_queue.put(("ERROR", self.transfert_id, "FILE_CREATION_ERROR"))
            return

        # NOTE: What exceptions does this throw?
        # FIXME (related): Implement download resuming somehow.
        try:
            last_time = time.time()
            for chunk in res.iter_content(self.blocksize):
                self.writer.enqueue(fd, chunk)
                this_time = time.time()
                self.event_queue.put((
                    "SPEED",
                    self.transfert_id,
                    len(chunk) / (1024.0 * (this_time - last_time))))
                self.add_perf_num((self.blocksize / 1024) / (this_time - last_time))
                last_time = this_time
                self.data_size += len(chunk)
                data_md5sum.update(chunk)
                if(self.abort):
                    raise Exception("Shutting down")
        except Exception as e:
            try:
                os.unlink(self.filename)
            except Exception as e:
                pass
            self.mark_end_time()
            self.event_queue.put(("ABORTED", self.transfert_id, 'Caught exception: ' + str(e)))
            return

        # Ensure the FD gets closed
        self.writer.enqueue(fd, "", last=True)
        self.mark_end_time()

        if data_md5sum.hexdigest() != self.checksum:
            os.unlink(self.filename)
            self.mark_end_time()
            self.event_queue.put(("ERROR", self.transfert_id, "CHECKSUM_MISMATCH_ERROR"))
            return

        # Note: Not closing the file is deliberate. The writer closes the file.
        self.event_queue.put((
            "DONE",
            self.transfert_id,
            (self.data_size / 1024) / (self.start_time - self.end_time)))

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
                log.exception("Error querying for new transfers; shutting down.")
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
