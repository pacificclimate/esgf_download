import sys
import time
import threading
import logging

from collections import deque

log = logging.getLogger(__name__)

class MultiFileWriter:
    '''
    A write serializer which allows for many files to be open but for only one
    to be written to at once. The goal of this is to keep filesystem thrash to
    a minimum while downloading.
    '''
    def __init__(self, max_queue_len=10):
        self.queue = deque()
        self.lock = threading.Lock()
        self.pool_full_sema = threading.BoundedSemaphore(max_queue_len)
        self.pool_empty_sema = threading.Semaphore(0)
        self.run_writer_thread = True
        log.debug("Writer starting...")
        self.writer_thread = threading.Thread(target=self.process, name="WriterThread")
        self.writer_thread.start()
            
    def process(self):
        '''
        Should only be called by the constructor at creation time. Performs the
        dequeueing and writing.
        '''
        while self.run_writer_thread:
            self.pool_empty_sema.acquire()
            with self.lock:
                fd, res, last = self.queue.popleft()
            self.pool_full_sema.release()
            fd.write(res)
            if last:
                fd.close()
        
    def enqueue(self, fd, res, last=False):
        '''
        Enqueues a block to be written to the specified fd.
        :param fd: The file descriptor to write to.
        :param res: The data to be written out.
        :param last: A flag to specify that this is the last block, and the file
            descriptor should be closed after it is written out.
        '''
        self.pool_full_sema.acquire()
        with self.lock:
            self.queue.append((fd, res, last))
        self.pool_empty_sema.release()

    def write_and_quit(self):
        '''
        Writes out remaining blocks in the queue and informs the writer thread
        that it should quit.
        '''
        # Wait until the queue is empty...
        while len(self.queue) > 0:
            time.sleep(1)
        
        self.run_writer_thread = False

        # Add a dummy entry after setting run_writer_thread to False so the
        # thread gets awakened and then can die peacefully.
        self.pool_full_sema.acquire();
        with self.lock:
            self.queue.append((sys.stdout, '', False))
        self.pool_empty_sema.release();
        self.writer_thread.join()
        log.debug("Writer exiting...")
