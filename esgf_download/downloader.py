import os
import time
import threading
import logging
import md5 ## FIXME: Use hashlib

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
