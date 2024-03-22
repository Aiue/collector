#!/usr/bin/env python3
# Common Crawl Collector
# Written by Jens Nilsson Sahlin for Linköping University 2024
# Additional details will be available in README.md

import time

# Configuration, using a dict for verbosity purposes.
config = {
    archive_host = 'data.commoncrawl.org',
    archive_list_uri = '/cc-index/collections/index.html',
    max_requests_limit = 5,
    max_requests_time = 5,
    cache_index_clusters = True,
}

# Global variable initiation.
lastRequests = []

# Classes
class Archive:
    def __init__(self, archiveID, indexPathsFile):
        self.archiveID = archiveID
        self.indexPathsFile = RemoteFile(indexPathsFile)

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
