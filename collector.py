#!/usr/bin/env python3
# Common Crawl Collector
# Written by Jens Nilsson Sahlin for Linköping University 2024
# Additional details will be available in README.md

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
