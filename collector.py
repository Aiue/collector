#!/usr/bin/env python3
# Common Crawl Collector
# Written by Jens Nilsson Sahlin for Linköping University 2024
# Additional details will be available in README.md

import logging
import os.path
import requests
import time

# Configuration, using a dict for verbosity purposes.
config = {
    archive_host = 'https://data.commoncrawl.org',
    archive_list_uri = '/cc-index/collections/index.html',
    max_requests_limit = 5,
    max_requests_time = 5,
    cache_index_clusters = True,
}

# Global variable initiation.
lastRequests = []
logger = logging.getLogger('collector') 

# Classes
class Archive:
    def __init__(self, archiveID, indexPathsFile):
        self.archiveID = archiveID
        self.indexPathsFile = RemoteFile(indexPathsFile)

    def __repr__(self):
        return self.archiveID

    def updatePaths(self):
        f = RemoteFile(indexPathsFile)
        for line in f.read(): # TODO: Catch errors.
            if line.endswith('cluster.idx'):
                self.clusterIndex = RemoteFile(line)
                i = line.rfind('cluster.idx')
                self.indexPathsURI = line[0:i]

class Archives:
    def __init__(self):
        self.archives = {}
        self.lastUpdate = 0

    def update(self):
        if time.time() - self.lastUpdate < 86400:
            return
        index = RemoteFile(config.archive_host + config.archive_list_uri)
        contents = index.read()
        # TODO: Parse the html response (or catch errors).
        self.lastUpdate = time.time()

class RemoteFile:
    def __init__(self, url, filename=None, offset=None, length=None):
        self.url = url
        self.filename = filename # Local filename, doubles as cache indicator.
        self.offset = offset
        self.length = length
        self.attempts = 0


        # RF.download() - calls get() AND write() ..but what if we have it, DUPLICATION
        # RF.read() - very special case
        #               if we have cache, read from cache
        #               otherwise, call get()
        #                 if we should cache, call write()
        # RF.write() - Writes to file.
        # RF.get() - Just handles the HTTP request.
    def download(self):
        # Just a wrapper, but it simplifies things.
        if not self.filename:
            logger.error('attempted to download file with no local filename set: %s', self.url)
        elif if os.path.exists(self.filename):
            logger.warning('attempted to download already existing file: %s', self.filename)
        else:
            try:
                contents = self.get()
            except Exception as error:
                # We do not need to raise it further.
                logger.error(error)
                # TODO: Add to retry queue. Needs a reference to it.
                return False
            self.write(contents)
            return True

    def read(self):
        # TODO: Writeme
        pass

class RetryQueue:
    def __init__(self):
        self.queue = [] # [RemoteFile(file1), RemoteFile(file2), ...]
        # TODO: Writeme
        
    def process(self):
        if len(self.queue) == 0:
            return
        self.queue.pop(0).download()

class Domain:
    def __init__(self, domain):
        self.domain = domain
        self.archives = {}

    def __repr__(self):
        return self.domain

    def loadHistory(self):
        # TODO: Writeme
        pass

    def updateHistory(self, archiveID, history): # TODO: Possibly use Archive object instead. Requires some additional rewriting.
        # TODO: Writeme
        pass

    def search(self, archive):
        # TODO: Writeme
        pass

    def searchClusters(self, clusters):
        # TODO: Writeme
        pass

    def getFile(self, index):
        # TODO: Writeme
        pass

class ClusterIndex:
    def __init__(self, url=None):
        # BE VERY, VERY CAREFUL WITH INITIALIZING WITH A CLUSTER INDEX URL
        # They're huge, and this should only ever be done through within
        # a memoized function.
        self.index = []
        if url:
            #TODO: Writeme
            pass
