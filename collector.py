#!/usr/bin/env python3
# Common Crawl Collector
# Written by Jens Nilsson Sahlin for Linköping University 2024
# Additional details will be available in README.md
# Licensed under GPLv3, see license.txt

import bisect
import gzip
import html.parser
import json
import logging
import os
import os.path
import requests
import time

# Configuration, using a dict for verbosity purposes.
class config:
    archive_host = 'https://data.commoncrawl.org'
    archive_list_uri = '/cc-index/collections/index.html'
    max_file_size = 104857600 # Max file size we'll download.
                               # Currently set to 100 MiB, which may seem ridiculously large in context.
                               # Only applies to [W]ARC files.
    max_requests_limit = 5
    max_requests_time = 5
    cache_index_clusters = True
    pywb_collection_dir = 'path/to/pywb/collection'
    domain_list_file = 'domains.txt'

# Global variable initiation.
logger = logging.getLogger('collector')
import sys
logging.basicConfig(level=20, stream=sys.stdout)

# Exceptions
class ParserError(Exception):
    pass

# Classes
class Archive:
    def __init__(self, archiveID, indexPathsFile):
        self.archiveID = archiveID
        self.indexPathsFile = RemoteFile(config.archive_host + indexPathsFile)

    def __repr__(self):
        return self.archiveID

    def updatePaths(self):
        logger.debug('Updating paths for %s.', self.archiveID)
        f = RemoteFile(self.indexPathsFile)
        try:
            contents = f.read()
        except Exception as error:
            logger.error('Could not read remote file %s: %s', self.indexPathsFile, error)
            raise
        else:
            for line in contents.decode().splitlines():
                if line.endswith('cluster.idx'):
                    self.clusterIndex = RemoteFile(config.archive_host + '/' + line, '.cache/' + self.archiveID + '/cluster.idx')
                    self.clusterIndex.bypass_decompression = True # Special case, 1 out of 2 files without compression.
                    i = line.rfind('cluster.idx')
                    self.indexPathsURI = line[0:i]
            if not self.clusterIndex:
                raise ParserError('Could not update paths for archive %s (incomplete or otherwise malformed paths file).', self.archiveID)

class Archives:
    def __init__(self):
        self.archives = {}
        self.lastUpdate = 0

    def __iter__(self):
        return iter(self.archives.items())

    class HTMLParser(html.parser.HTMLParser):
        archiveID = None
        tdCount = 0
        def __init__(self):
            self.archives = []
            self.isScanningTableBody = False
            super().__init__()

        def handle_starttag(self, tag, attributes):
            if tag == "tbody":
                self.isScanningTableBody = True
            if self.isScanningTableBody:
                if tag == 'tr':
                    self.archiveID = None
                    self.tdCount = 0

                if tag == 'td':
                    self.tdCount += 1

                if tag == "a":
                    if self.tdCount == 4:
                        # As of writing, only 'href' attributes are present.
                        # But in case this changes in the future, let's be vigilant.
                        for attribute in attributes:
                            if attribute[0] == 'href':
                                self.archives.append(Archive(self.archiveID, attribute[1]))
                
        def handle_data(self, data):
            if self.tdCount == 1 and self.archiveID == None:
                # This is all the handling we need. The first trigger of this will be the archive ID.
                # There will be a second trigger with a newline, for unknown reason. This is why we need this handling.
                self.archiveID = data

    def update(self):
        if time.time() - self.lastUpdate < 86400:
            return
        logger.debug('Updating archive list.')
        index = RemoteFile(config.archive_host + config.archive_list_uri)
        index.bypass_decompression = True # Hack for this one special case (and one more)
        try:
            contents = index.read()
        except Exception as error:
            raise
        else:
            parser = self.HTMLParser()
            parser.feed(contents.decode())
            if len(parser.archives) == 0:
                raise ParserError('Could not parse archive list.')
            for archive in parser.archives:
                if archive.archiveID not in self.archives:
                    logger.info('New archive: %s', archive.archiveID)
                    self.archives[archive.archiveID] = archive

            parser.close()
            self.lastUpdate = time.time()

class RemoteFile:
    lastRequests = []

    def __init__(self, url, filename=None, offset=None, length=None): #TODO: Add digest information.
        self.url = url
        self.filename = filename # Local filename, doubles as cache indicator.
        self.offset = offset
        self.length = length
        self.attempts = 0

    def __repr__(self):
        return self.url
        
    def download(self):
        logger.debug('Downloading from %s', self.url)
        # Essentially just a wrapper, but it simplifies things.
        if not self.filename:
            logger.error('Attempted to download file with no local filename set: %s', self.url)
        elif os.path.exists(self.filename):
            logger.warning('Attempted to download already existing file: %s', self.filename)
        else:
            try:
                contents = self.get()
            except Exception as error:
                # We do not need to raise it further.
                logger.error('Could not download file from %s: %s', url, error)
                rq = RetryQueue()
                rq.add(self)
                raise # Raising it anyway. But we shouldn't. Leaving for now. TODO: 
            else:
                # TODO: Add digest check here.
                try:
                    self.write(contents)
                except Exception as error:
                    rq = RetryQueue()
                    rq.add(self)
                    logger.error('Could not write file \'%s\': %s', self.filename, error)
                    raise

    def read(self):
        logger.debug('Reading from %s', self.url)
        if self.filename and os.path.exists(self.filename): # File is in cache.
            logger.debug('File is cached, reading from %s', self.filename)
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
                if hasattr(self, 'filename') and self.filename: # We should cache file.
                    try:
                        self.write(contents)
                    except Exception as error:
                        logger.warning('Could not write cache file \'%s\': %s', self.filename, error)
                        raise
        if hasattr(self, 'bypass_decompression'): # special case for main index
            return contents
        return gzip.decompress(contents)

    def write(self, contents):
        if not hasattr(self, 'filename'):
            raise RuntimeError('RemoteFile.write() called with no filename set: %s', url)
        logger.debug('Writing from %s to %s', self.url, self.filename)
        if '/' in self.filename:
            left,right = self.filename.rsplit('/', 1)
            if not os.path.exists(left):
                logger.info("Recursively creating directory '%s'.", left)
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

    def get(self):
        logger.debug('Getting from %s', self.url)
        if len(self.lastRequests) > 0:
            logger.debug('Time difference from now and previous request #%d is %f seconds.', len(self.lastRequests), time.time() - self.lastRequests[0])
        if len(self.lastRequests) >= config.max_requests_limit:
            diff = time.time() - self.lastRequests[0]
            if diff < config.max_requests_time:
                diff = config.max_requests_time - diff
                logger.info('Request limit reached, sleeping for %f seconds.', diff)
                time.sleep(diff)
            self.lastRequests.pop(0)

        headers = None # Should not need to be initialized/emptied, but do it anyway.
        if self.offset and self.length:
            headers = {'Range': "bytes=" + str(self.offset) + "-" + str(self.offset+self.length-1)}
        try:
            r = requests.get(self.url, headers=headers)
        except Exception:
            raise
        if r.status_code != 200 and r.status_code != 206: # We need to also allow 206 'partial content'
            raise Exception('Failed to get %s: %i %s', self.url, r.status_code, r.reason)

        self.lastRequests.append(time.time())
        return r.content

class RetryQueue:
    _instance = None

    def __new__(self):
        if self._instance == None:
            self._instance = super(RetryQueue, self).__new__(self)
        return self._instance

    def __init__(self):
        if hasattr(self, 'queue'):
            return # This instance is already initialised.
                   # I would prefer a cleaner way of doing this, but alas.
        self.queue = [] # [RemoteFile(file1), RemoteFile(file2), ...]

        if os.path.exists('retryqueue'):
            try:
                f = open('retryqueue', 'r')
            except Exception as error:
                # TODO: Do we want to exit here to prevent data loss?
                log.error('Could not load retry queue: %s', error)
                raise
            else:
                for line in f:
                    url,filename,offset,length,attempts = line.split('\t')
                    self.add(RemoteFile(url, filename, int(offset), int(length)))
                    self.queue[len(self.queue)-1].attempts = int(attempts) # Not the prettiest way of doing it, but this one case
                                                        # does not warrant __init__ inclusion.
                f.close()
                logger.info('Loaded retry queue with %d items.', len(self.queue))

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
            raise
        else:
            for item in self.queue:
                f.write(item.url + '\t' + item.filename + '\t' + str(item.offset) + '\t' + str(item.length) + '\t' + str(item.attempts) + '\n')
            f.close()

class Domain:
    memoizeCache = {}
    
    def __init__(self, domain):
        logger.debug('New domain: %s', domain)
        if '/' in domain:
            raise RuntimeError('Domains cannot contain \'/\'') # May (and with 'may', I mean 'will') want to adress this differently.
                                                               # The main reason would be how we save history.
                                                               # Strongly consider changing naming scheme to adress this.
        self.domain = domain
        self.loadHistory()
        self.searchString = ""
        uri = ""
        if '/' in domain:
            domain,uri = domain.split('/', 1)
        domain = domain.split('.')
        for i in range(len(domain),0,-1):
            self.searchString += domain[i-1]
            if i > 1:
                self.searchString += ','
        self.searchString += ')/' + uri # TODO: Consider skipping this to include subdomains.
                                        # May want additional steps to allow for wildcard matching.
                                        # Caveat: only left-side wildcards (*.domain.com) would be
                                        # workable with binary search, and simply removing ')/'+uri would
                                        # make those implicit.

    def __repr__(self):
        return self.domain

    def loadHistory(self):
        logger.debug('Loading history for %s', self.domain)
        if os.path.exists('history/' + self.domain):
            try:
                f = open('history/' + self.domain, 'r')
            except Exception:
                raise
            else:
                self.history = json.load(f) #TODO: Add exception handling
                logger.info('Loaded search history for %s', self.domain)
        else:
            self.history = {}

    def updateHistory(self, archiveID, history): # TODO: Possibly use Archive object instead. Requires some additional rewriting.
        logger.debug('Updating history for %s', self.domain)
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
        logger.debug('Searching %s for %s', archive.archiveID, self.domain)
        if 'search' in self.memoizeCache and self.memoizeCache['search'][0] == archive:
            return self.memoizeCache['search'][1]

        results = []
        index = []
        if not hasattr(archive, 'clusterIndex'): # Implies indexPathsURI is also empty
            try:
                archive.updatePaths()
            except Exception:
                raise
        try:
            for line in archive.clusterIndex.read().decode().splitlines():
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
            self.memoizeCache['search'] = (archive, results)
            return results

    def searchClusters(self, archive, clusters): # TODO: Not happy with variable names here. Need to revisit and rename.
        logger.debug('Searching %s clusters for %s', archive.archiveID, self.domain)
        if 'searchClusters' in self.memoizeCache and self.memoizeCache['searchClusters'][0] == self and self.memoizeCache['searchClusters'][1] == archive:
            return self.memoizeCache['searchClusters'][2]

        results = []
        # TODO: (maybe)
        # This method has the potential to create very large lists. But unless we're matching against an entire
        # top domain, we shouldn't get anywhere near the size of a cluster index. Even so, it may be worth
        # considering a rewrite.
        for cluster in clusters:
            # We do not need a call to Archive.updatePaths() here, we should only get here after Domain.search()
            index = []
            if config.cache_index_clusters:
                cacheFileName = '.cache/' + archive.archiveID + '/' + cluster[2] + '-' + str(cluster[5])
            indexFile = RemoteFile(
                config.archive_host + '/' + archive.indexPathsURI + cluster[2],
                cacheFileName,
                cluster[3],
                cluster[4])
            try:
                for line in indexFile.read().decode().splitlines():
                    searchable_string,timestamp,json = line.split(' ', 2)
                    index.append((searchable_string, int(timestamp), json))
            except Exception:
                raise
            else:
                position = bisect.bisect_left(index, (self.searchString, 0, ""))
                # Unlike the cluster index, there should be no earlier result than position.
                while position < len(index):
                    if index[position][0].startswith(self.searchString):
                        # Only the json data will be interesting from here on.
                        results.append(index[position][2])
                        position += 1
                    else:
                        break
        self.memoizeCache['searchClusters'] = (self, archive, results)
        if len(results) == 0:
            self.updateHistory(archive.archiveID, True)
        logger.debug('Found %d search results for %s/%s.', len(results), self.domain, archive.archiveID)
        return results

    def getFile(self, archive, index):
        # First, determine what to fetch.
        if archive.archiveID not in self.history:
            position = 0
        elif type(self.history[archive.archiveID]) == bool:
            # This shouldn't ever happen here. But let's catch it anyway.
            raise RuntimeError('Attempted to download completed domain/archive combination: %s %s', self.domain, archive.archiveID)
        elif type(self.history[archive.archiveID]) == int:
            position = self.history[archive.archiveID] + 1

        logger.debug('Result found at %d', position)

        logger.debug('Loading JSON: %s', index[position])
        # Everything is treated as strings, so we will need to convert integers.
        fileInfo = json.loads(index[position])

        if int(fileInfo['length']) > config.max_file_size:
            logger.info('Skipping download of %s as file exceeds size limit at %s bytes.', fileInfo['filename'], fileInfo['length'])
        else:
            filerange = '-' + fileInfo['offset'] + '-' + str(int(fileInfo['offset'])+int(fileInfo['length'])-1)

            filename = config.pywb_collection_dir + '/' + archive.archiveID + '-'
            if fileInfo['filename'].endswith('.arc.gz'):
                for name in fileInfo['filename'].split('/'):
                    filename += name
                    filename = filename[0:len(filename)-7] + filerange + '.arc.gz'
            elif fileInfo['filename'].endswith('.warc.gz'):
                _,_,_,partial_path,_,warcfile = fileInfo['filename'].split('/')
                filename += partial_path + '-' + warcfile[0:len(warcfile)-8] + filerange + '.warc.gz'

                url = config.archive_host + '/' + fileInfo['filename']
                rf = RemoteFile(url, filename, int(fileInfo['offset']), int(fileInfo['length']))
                logger.info('Downloading from %s (range %i-%i) to %s', url, int(fileInfo['offset']), int(fileInfo['offset'])+int(fileInfo['length'])-1, filename)
                rf.download()

        #TODO: Exception handling below
        if position == len(index)-1:
            self.updateHistory(archive.archiveID, True)
        else:
            self.updateHistory(archive.archiveID, position)

#

def main():
    logger.info('Collector running.')
    archives = Archives()
    domains = []
    logger.debug('Reading domain list.')
    try:
        f = open(config.domain_list_file, 'r')
    except Exception as error:
        logger.critical('Could not read \'%s\': %s', config.domain_list_file, error)
        raise
    for line in f.read().splitlines():
        domains.append(Domain(line))
    f.close()

    if len(domains) == 0:
        logger.critical('No domains loaded, exiting.')
        raise RuntimeError('No domains loaded.')

    logger.debug('Loading retry queue.')
    retryqueue = RetryQueue()

    while True:
        try:
            archives.update()
        except Exception as error: # TODO: Treat different exceptions .. differently. Will require some additional rewriting.
                                   # The key thing is that for some exceptions we'll want to continue after a call to sleep.
            logger.error('Could not update archives: %s', error)
            raise

        retryqueue.process()

        archive = None
        domain = None
        for d in domains:
            # Not the most elegant solution, but we'll want a double break somehow.
            if domain:
                break
            for _,a in archives:
                # 1 will equal True, so instead, we will have to do a type comparison.
                if not a.archiveID in d.history or type(d.history[a.archiveID]) == int:
                    domain = d
                    archive = a
                    break

        if not domain:
            # Sleep until next archive list update.
            time_to_sleep = time.time() - archives.lastUpdate + 86400
            logger.info('All searches currently finished, sleeping until next archive list update in %.2f seconds.', time_to_sleep)
            time.sleep(time_to_sleep)
            continue

        # TODO: These will all require exception handling.
        results = domain.search(archive)
        if len(results) > 0:
            results = domain.searchClusters(archive, results)
            if len(results) > 0:
                domain.getFile(archive, results)

if __name__ == "__main__":
    main()
