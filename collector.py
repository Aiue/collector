#!/usr/bin/env python3
# Common Crawl Collector
# Written by Jens Nilsson Sahlin for Linköping University 2024
# Additional details will be available in README.md
# Licensed under GPLv3, see license.txt

import gzip
import html.parser
import json
import logging
import os
import os.path
import requests
import time

# Configuration, using a dict for verbosity purposes.
config = {
    'archive_host': 'https://data.commoncrawl.org',
    'archive_list_uri': '/cc-index/collections/index.html',
    'max_file_size': 104857600, # Max file size we'll download.
                                # Currently set to 100 MiB, which may seem ridiculously large in context.
                                # Only applies to [W]ARC files.
    'max_requests_limit': 5,
    'max_requests_time': 5,
    'cache_index_clusters': True,
    'pywb_collection_dir':  'path/to/pywb/collection',
}

# Global variable initiation.
logger = logging.getLogger('collector')

# Classes
class Archive:
    def __init__(self, archiveID, indexPathsFile):
        self.archiveID = archiveID
        self.indexPathsFile = RemoteFile(indexPathsFile)

    def __repr__(self):
        return self.archiveID

    def updatePaths(self):
        try:
            f = RemoteFile(self.indexPathsFile)
        except Exception as error:
            logger.error('Could not read remote file %s: %s', self.indexPathsFile, error)
        else:
            for line in f.read():
                if line.endswith('cluster.idx'):
                    self.clusterIndex = RemoteFile(line, '.cache/' + self.archiveID + '/cluster.idx')
                    i = line.rfind('cluster.idx')
                    self.indexPathsURI = line[0:i]
            if not self.clusterIndex:
                raise Exception('Could not update paths for archive %s.', self.archiveID)

class Archives:
    def __init__(self):
        self.archives = {}
        self.lastUpdate = 0

    class HTMLParser:
        # Catch everything from <tbody> to </tbody>.
        # Groups of 3 <a>:
        #  1: archiveID
        #  2: we don't care about this one
        #  3: cc-index.paths.gz
        def __init__(self):
            self.archives = []
            self.isScanningTableBody = False
            self.linkCounter = 0

        def handle_starttag(self, tag, attributes):
            if tag == "tbody":
                self.isScanningTableBody = True
            if self.isScanningTableBody:
                if tag == "a":
                    linkCounter += 1
                    if linkCounter == 3:
                        # As of writing, only 'href' attributes are present.
                        # But in case this changes in the future, let's be vigilant.
                        for attribute in attributes:
                            if attribute[0] == "href":
                                self.indexPathsFile = attribute[1]

        def handle_endtag(self, tag):
            if tag == "a" and self.linkCounter == 3:
                logger.debug('Parsed archive %s %s', self.archiveID, self.indexPathsFile)
                self.linkCounter = 0
                self.archives.append(Archive(self.archiveID, self.indexPathsFile))
                self.archiveID = None
                self.indexPathsFile = None
                
            elif tag == "tbody":
                self.close()

        def handle_data(self, data):
            if linkCounter == 1:
                self.archiveID = data
            
        
    def update(self):
        if time.time() - self.lastUpdate < 86400:
            return
        index = RemoteFile(config.archive_host + config.archive_list_uri)
        index.bypass_decompression = True # Hack for this one special case
        try:
            contents = index.read()
        except Exception as error:
            raise
        else:
            parser = self.HTMLParser()
            parser.feed(contents)
            if len(parser.archives) == 0:
                raise Exception('Could not parse archive list.')
            for archive in parser.archives:
                if not self.archives[archive.archiveID]:
                    logger.info('New archive: %s', archive.archiveID)
                    self.archives[archive.archiveID] = archive

            self.lastUpdate = time.time()

class RemoteFile:
    lastRequests = []

    def __init__(self, url, filename=None, offset=None, length=None): #TODO: Add digest information.
        self.url = url
        self.filename = filename # Local filename, doubles as cache indicator.
        self.offset = offset
        self.length = length
        self.attempts = 0

    def download(self): #TODO: Add digest check.
        # Just a wrapper, but it simplifies things.
        if not self.filename:
            logger.error('attempted to download file with no local filename set: %s', self.url)
        elif os.path.exists(self.filename):
            logger.warning('attempted to download already existing file: %s', self.filename)
        else:
            try:
                contents = self.get()
            except Exception as error:
                # We do not need to raise it further.
                logger.error('could not download file from %s: %s', url, error)
                # TODO: Add to retry queue. Needs a reference to it.
            self.write(contents)

    def read(self):
        if self.filename and os.path.exists(self.filename): # File is in cache.
            try:
                f = open(self.filename, 'rb')
            except Exception as error:
                raise
            else:
                contents = f.read()
                f.close()
        else:
            try:
                contents = self.get()
            except Exception as error:
                raise
            else:
                if self.filename: # We should cache file.
                    try:
                        self.write(contents)
                    except Exception as error:
                        logger.warning('Could not write cache file \'%s\': %s', filename, error)
        if self.bypass_decompression: # special case for main index
            return contents
        return gzip.decompress(contents)

    def write(self, contents):
        if not self.filename:
            raise Exception('RemoteFile.write() called with no filename set: %s', url)
        if '/' in self.filename:
            left,right = self.filename.rsplit('/', 1)
            if not os.path.exists(left):
                log.info("Recursively creating directory '%s'.", left)
            try:
                os.makedirs(left)
            except Exception:
                raise
        try:
            f = open(self.filename, 'wb')
        except Exception:
            raise
        f.write(contents)
        f.close()

    def get():
        if len(self.lastRequests) >= config.max_requests_limit:
            diff = time.time() - config.max_requests_time
            if self.lastRequests[0] < diff:
                logger.info('request limit reached, sleeping for %f seconds.', diff)
                time.sleep(diff)
            self.lastRequests..pop(0)

        headers = None # Should not need to be initialized/emptied, but do it anyway.
        if not self.offset or not self.length: # No need to error handle if only one is set.
            # The below may be uglier than other formatting according to some standards, but
            # in emacs it looks better this way!
            headers = {'Range': "bytes=" + str(offset) + "-" + str(offset+length-1)}
        try:
            r = requests.get(self.url, headers=headers)
        except Exception:
            raise
        if r.status_code != 200:
            raise Exception('Failed to get %s: %i %s', url, r.status_code, r.reason)
        else:
            self.lastRequests.append(time.time())
            return r.content

class RetryQueue:
    def __init__(self):
        self.queue = [] # [RemoteFile(file1), RemoteFile(file2), ...]

        if os.path.exists('retryqueue'):
            try:
                f = open('retryqueue', 'r')
            except Exception as error:
                log.error('Could not load retry queue: %s', error)
            else:
                for line in f:
                    url,filename,offset,length,attempts = line.split('\t')
                    self.add(RemoteFile(url, filename, int(offset), int(length)))
                    self.queue.attempts = int(attempts) # Not the prettiest way of doing it, but this one case
                                                    # does not warrant __init__ inclusion.
                f.close()
                log.info('Loaded retry queue with %d items.', len(self.queue))

    def process(self):
        if len(self.queue) == 0:
            return
        self.queue.pop(0).download()
        self.save()

    def add(self, item):
        self.queue.append(item)
        
    def save(self):
        try:
            f = open('retryqueue', 'w')
        except Exception as error:
            log.error('Could not write retry queue: %s', error)
        else:
            for item in self.queue:
                f.write(item.url + '\t' + item.filename + '\t' + str(item.offset) + '\t' + str(item.length) + '\t' + str(item.attempts))
            f.close()

class Domain:
    memoizeCache = {}
    
    def __init__(self, domain):
        if '/' in domain:
            raise Exception('Domains cannot contain \'/\'') # May want to adress this differently.
        self.domain = domain
        self.history = {}
        self.searchString = ""
        left,right = domain.split('/', 1)
        left = left.split('.')
        for i in range(len(left),0,-1):
            self.searchString += left[i-1]
            if i > 1:
                self.searchString += ','
        self.searchString += ')/' + right # TODO: Consider skipping this to include subdomains.
                                          # May want additional steps to allow for wildcard matching.
                                          # Caveat: only left-side wildcards (*.domain.com) would be
                                          # workable with binary search, and simply removing this would
                                          # make those implicit.

    def __repr__(self):
        return self.domain

    def loadHistory(self):
        if os.path.exists('history/' + self.domain):
            try:
                f = open('history/' + self.domain, 'r')
            except Exception:
                raise
            else:
                self.history = json.load(f)
                logger.info('Loaded search history for %s', self.domain)

    def updateHistory(self, archiveID, history): # TODO: Possibly use Archive object instead. Requires some additional rewriting.
        self.history[archiveID] = history
        if not os.path.exists('history'):
            os.mkdir('history')
        try:
            f = open('history/' + self.domain, 'w')
        except Exception:
            raise
        else:
            json.dump(self.history, f)
            # No log message, we might do this often.

    # Search functions are here rather than on the classes they operate on for cache purposes.
    # Rather than having their own classes*, actually.
    def search(self, archive):
        if memoizeCache.search and memoizeCache.search[0] == archive:
            return memoizeCache.search[1]

        results = []
        index = []
        try:
            for line in archive.clusterIndex.read():
                searchable_string,rest = line.split(' ')
                timestamp,filename,offset,length,cluster = rest.split('\t')
                index.append(
                    (searchable_string, # 0
                     int(timestamp),    # 1
                     filename,          # 2
                     int(offset),       # 3
                     int(length),       # 4
                     int(cluster)       # 5
                    ))
        except Exception:
            raise
        else:
            # This search format should mean we're always left of anything matching our search string.
            position = bisect.bisect_left(index, (self.searchString, 0, "", 0, 0, 0))
            # We may (and likely will) have matches in the index cluster prior to our match.
            results.append(index[position-1])
            while position < len(index):
                if index[position][0].startswith(self.searchString):
                    results.append(index[position])
                    position += 1
                else:
                    break
                memoizeCache.search = (archive, results)
                return results

    def searchClusters(self, archive, clusters): # #TODO: Not happy with variable names here. Need to revisit and rename.
        if memoizeCache.searchClusters and memoizeCache.searchClusters[0] == self and memoizeCache.searchClusters[1] == archive:
            return memoizeCache.searchClusters[2]

        results = []
        # TODO: (maybe)
        # This method has the potential to create very large lists. But unless we're matching against an entire
        # top domain, we shouldn't get anywhere near the size of a cluster index. Even so, it may be worth
        # considering a rewrite.
        for cluster in clusters:
            index = []
            if config.cache_index_clusters:
                cacheFileName = '.cache/' + archive.archiveID + '/' + cluster[2] + '-' + cluster[5]
            indexFile = RemoteFile(
                archive.indexPathsURI + cluster[2],
                cacheFileName,
                cluster[3],
                cluster[4])
            try:
                for line in indexFile.read():
                    searchable_string,timestamp,json = line.split(' ')
                    index.append((searchable_string, int(timestamp), json))
            except Exception:
                raise
            else:
                position = bisect.bisect_left(index, (self.searchString, 0, ""))
                # Unlike the cluster index, there should be no earlier result than position.
                while position < len(index):
                    if index[position][0].startswith(self.searchStringi):
                        # Only the json data will be interesting from here on.
                        results.append(index[position][2])
                    else:
                        break
        memoizeCache.searchClusters = (self, archive, results)
        return results

    def getFile(self, archive, index):
        # First, determine what to fetch.
        if not self.history[archive.archiveID]:
            position = 0
        elif type(self.history[archive.archiveID]) == bool:
            # This shouldn't ever happen here. But let's catch it anyway.
            raise Exception('Attempted to download completed domain/archive combination: %s %s', self.domain, archive.archiveID)
        elif type(self.history[archive.archiveID]) == int:
            position = self.history[archive.archiveID] + 1

        fileInfo = json.loads(index[position])

        if fileInfo.length > config.max_file_size:
            logger.info('Skipping download of %s as file exceeds size limit at %d bytes.', fileInfo.filename, fileInfo.length)
        else:
            filerange = '-' + str(fileInfo.offset) + '-' + str(fileInfo.offset+fileInfo.length-1)

            filename = pywb_collection_dir + '/' + archive.archiveID + '-'
            if fileInfo.filename.endswith('.arc.gz'):
                for name in fileInfo.filename.split('/'):
                    filename += name
                    filename = filename[0:len(filename)-7] + filerange + '.arc.gz'
            elif fileInfo.filename.endswith('.warc.gz'):
                _,_,_,partial_path,_,warcfile = fileInfo.filename.split('/')
                filename += partial_path + '-' + warcfile[0:len(warcfile)-8] + filerange + '.warc.gz'

                url = config.archive_host + '/' + fileInfo.filename
                rf = RemoteFile(url, filename, fileInfo.offset, fileInfo.length)
                logger.info('Downloading from %s (range %i-%i) to %s', url, fileInfo.offset, fileInfo.offset+fileInfo.length-1, filename)
                rf.download()

        if position == len(index)-1:
            self.updateHistory(archive.archiveID, True)
        else:
            self.updateHistory(archive.archiveID, position)
