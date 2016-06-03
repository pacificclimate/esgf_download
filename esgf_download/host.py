import os
import requests

from collections import deque

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
