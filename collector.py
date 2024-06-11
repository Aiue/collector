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
import logging.config
import logging.handlers # Not used by default, but allows additional config options.
import os
from pathlib import Path
import requests
import time

# I don't like the configuration file alternatives python offers. I'll write my own.
class Config:
    # Set defaults
    archive_host = 'https://data.commoncrawl.org'
    archive_list_uri = '/cc-index/collections/index.html'
    max_file_size = 104857600 # Max file size we'll download.
                               # Currently set to 100 MiB, which may seem ridiculously large in context.
                               # Only applies to [W]ARC files.
    max_requests_limit = 1
    max_requests_time = 5
    cache_index_clusters = False
    pywb_collection_dir = 'path/to/pywb/collection' # Should (probably) also be Pathified.
                                                    # However, this would require more extensive rewrites.
    domain_list_file = Path('domains.conf')
    critical_warn_interval = 8 # Warns (critical log) every n hours if everything keeps failing.
    safe_path = Path.cwd()

    def __init__(self, configFile):
        if configFile.exists():
            with configFile.open('r') as f:
                for line in f.read().splitlines():
                    # This isn't pretty, but it will ensure the preferred format is viable.
                    key,value = line.split('=')
                    if key == 'cache_index_clusters':
                        value = bool(value)
                    elif key in ['domain_list_file', 'safe_path']:
                        value = Path(value)
                    elif key in ['max_file_size', 'max_requests_limit', 'max_requests_time', 'critical_warn_interval']:
                        value = int(value)
                    setattr(self, key, value)

config = Config(Path('collector.conf'))

# Init logger
logger = logging.getLogger()
logging.config.fileConfig('logger.conf')

# Exceptions
class ParserError(Exception):
    pass

class BadHTTPStatus(Exception):
    pass

# Utility functions
def path_is_safe(path, inst=None): # path is a Path.
    if (
            '/../' in str(path)
         or str(path).startswith('../')
         or str(path) == '..'
         or str(path).endswith('/..')
         or path.is_absolute()
            and not (
                str(path).startswith(config.pywb_collection_dir)
             or str(path).startswith(str(config.safe_path))
    )):
        msg = f"Unsafe path: {self}"
        if inst and type(inst) == RemoteFile: # Type is either RemoteFile or Domain. Only RemoteFile has attributes we want to add.
            msg += ' (' + str(self.url) + ')'  # Only url is of real interest.
            
        logger.warning(msg)
        raise ValueError(msg) # Yes, it could lead to a deadlock. But if we ever do end up here, we have bigger issues.
        return False # Won't do anything, but if we remove the raise, this will need to be here.
    return True

def is_match(entry, search):
    return entry.startswith(search + ')') or entry.startswith(search + ',')

def get_domain(domain):
    for d in Domain.domains:
        if d.domain == domain:
            return d

# Classes
class Archive:
    def __init__(self, archiveID, indexPathsFile):
        self.archiveID = archiveID
        self.indexPathsFile = RemoteFile(config.archive_host + indexPathsFile)
        self.clusterIndex = None

    def __repr__(self):
        return self.archiveID

    def updatePaths(self):
        logger.debug('Updating paths for %s.', self.archiveID)
        f = RemoteFile(self.indexPathsFile)
        contents = f.read()
        for line in contents.splitlines():
            if line.endswith('cluster.idx'):
                self.clusterIndex = RemoteFile(config.archive_host + '/' + line, '.cache/' + self.archiveID + '/cluster.idx')
                self.clusterIndex.bypass_decompression = True # Special case, 1 out of 2 files without compression.
                i = line.rfind('cluster.idx')
                self.indexPathsURI = line[0:i]
        if not self.clusterIndex:
            logger.critical('Could not update paths for archive %s (incomplete or otherwise malformed paths file).', self.archiveID)
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
        initial = False
        if len(self.archives) == 0:
            initial = True
        if time.time() - self.lastUpdate < 86400:
            return
        logger.debug('Updating archive list.')
        index = RemoteFile(config.archive_host + config.archive_list_uri)
        index.bypass_decompression = True # Hack for this one special case (and one more)
        contents = index.read()
        parser = self.HTMLParser()
        parser.feed(contents)
        if len(parser.archives) == 0:
            logger.critical('Could not parse archive list.')
            raise ParserError('Could not parse archive list.')
        for archive in parser.archives:
            if archive.archiveID not in self.archives:
                if not initial:
                    logger.info('New archive: %s', archive.archiveID)
                self.archives[archive.archiveID] = archive
                with Path('archive_count').open('w') as f:
                    f.write(str(len(self.archives)))

        parser.close()
        self.lastUpdate = time.time()
        if initial:
            logger.info('Found %d archives.', len(self.archives))

class RemoteFile:
    lastRequests = []

    def __init__(self, url, filename=None, offset=None, length=None, domain=None, archiveID=None):
        self.url = url
        if filename and path_is_safe(Path(filename), self): # Local filename, doubles as cache indicator.
            self.filename = Path(filename)
        else:
            self.filename = None
        self.offset = offset
        self.length = length
        self.attempts = 0
        self.bypass_decompression = False
        # The following two are for status tracking
        self.domain=domain
        self.archiveID = archiveID

    def __repr__(self):
        return self.url

    def download(self):
        #logger.debug('Downloading from %s to %s', self.url, str(self.filename))
        # Essentially just a wrapper, but it simplifies things.
        if not self.filename:
            logger.error('Attempted to download file with no local filename set: %s', self.url)
        elif self.filename.exists():
            if self.length and self.filename.stat().st_size < self.length:
                logger.info('Restarting incomplete download from %s to %s', self.url, self.filename)
            else:
                logger.warning('Attempted to download already existing file:')
                logger.warning('  Filename: %s', self.filename)
                logger.warning('  URL: %s', self.url)
                logger.warning('  Size (local): %d bytes', self.filename.stat().st_size)
                logger.warning('  Size (remote): %d bytes', self.length)
                return
        try:
            contents = self.get()
        except (requests.RequestException, BadHTTPStatus) as error:
            rq = RetryQueue()
            rq.add(self)
            raise
        else:
            self.write(contents)

    def read(self):
        logger.debug('Reading from %s', self.url)
        contents = None
        if self.filename and self.filename.exists(): # File is in cache.
            if self.length:
                size = self.length
            else:
                r = requests.head(self.url)
                size = int(r.headers['Content-Length'])

            fsize = self.filename.stat().st_size
            if fsize == size:
                logger.debug('File is cached, reading from %s', self.filename)
                with self.filename.open('rb') as f:
                    contents = f.read()
            else:
                logger.debug('Cache file is %d bytes, remote file is %d bytes. Redownloading.', fsize, size)
                self.filename.unlink()

        if not contents:
            contents = self.get()
            if self.filename: # We should cache file.
                self.write(contents)
        if self.bypass_decompression: # special case for main index
            return contents.decode()
        return gzip.decompress(contents).decode()

    def write(self, contents):
        if not self.filename:
            raise RuntimeError('RemoteFile.write() called with no filename set: %s', url)
        logger.debug('Writing from %s to %s', self.url, self.filename)
        if not self.filename.parents[0].exists():
            logger.info("Recursively creating directory '%s'.", self.filename.parents[0])
            self.filename.parents[0].mkdir(parents=True)
        with self.filename.open('wb') as f:
            f.write(contents)

    def get(self):
        logger.debug('Getting from %s', self.url)
        if len(self.lastRequests) > 0:
            logger.debug('Time difference from now and previous request #%d is %f seconds.', len(self.lastRequests), time.time() - self.lastRequests[0])
        if len(self.lastRequests) >= config.max_requests_limit:
            diff = time.time() - self.lastRequests[0]
            if diff < config.max_requests_time:
                diff = config.max_requests_time - diff
                logger.debug('Request limit reached, sleeping for %f seconds.', diff)
                time.sleep(diff)
            self.lastRequests.pop(0)

        headers = None # Should not need to be initialized/emptied, but do it anyway.
        if self.offset and self.length:
            headers = {'Range': "bytes=" + str(self.offset) + "-" + str(self.offset+self.length-1)}
        self.lastRequests.append(time.time())
        try:
            r = requests.get(self.url, headers=headers)
        except requests.RequestException as error:
            logger.error('Could not get %s - %s', self.url, error)
            raise
        if r.status_code != 200 and r.status_code != 206: # We need to also allow 206 'partial content'
            logger.error('Bad HTTP response %d %s for %s', r.status_code, r.reason, self.url)
            raise BadHTTPStatus(self.url, self.offset, self.length, r.status_code, r.reason)

        return r.content

class RetryQueue:
    # Overall, very hack quality. But it will do.
    queue = [] # [RemoteFile(file1), RemoteFile(file2), ...]

    def load(self):
        if Path('retryqueue').exists():
            with open('retryqueue', 'r') as f:
                for line in f:
                    url,filename,offset,length,domain,archiveID,attempts = line.split('\t')
                    self.add(RemoteFile(url, Path(filename), int(offset), int(length), domain, archiveID), True)
                    self.queue[len(self.queue)-1].attempts = int(attempts) # Not the prettiest way of doing it, but this one case
                                                                           # does not warrant __init__ inclusion.
                logger.info('Loaded retry queue with %d items.', len(self.queue))

    def process(self):
        if len(self.queue) == 0:
            return
        item = self.queue[0]
        domain = get_domain(item.domain)
        if not domain:
            raise RuntimeError('Unknown domain in retry queue: %s %s %s', item.url, item.filename, item.domain)
        domain.updateHistory(item.archiveID, 'failed', domain.history[item.archiveID]['failed'] - 1)
        self.queue.pop(0).download()
        self.save()

    def add(self, item, no_history=None):
        if not no_history:
            domain = get_domain(item.domain)
            if not domain:
                logger.warning('\'%s\' is no longer in domain list, removing item from retry queue: %s -> %s', item.domain, item.url, item.filename)
                return # This domain is no longer on our list.
            # A slightly convoluted construction.
            domain.updateHistory(item.archiveID, 'failed', domain.history[item.archiveID]['failed'] + 1)
        self.queue.append(item)
        self.save()
        
    def save(self):
        with open('retryqueue', 'w') as f:
            for item in self.queue:
                f.write(item.url + '\t' + str(item.filename) + '\t' + str(item.offset) + '\t' + str(item.length) + '\t' + item.domain + '\t' + item.archiveID + '\t' + str(item.attempts) + '\n')
            f.close()

class Domain:
    memoizeCache = {}
    domains = []
    
    def __init__(self, domain): # TODO: Check that it's not a duplicate.
        logger.debug('New domain: %s', domain)
        if not '.' in domain: # Additional validation will follow when building the search string.
                              # We don't need to be super strict with the verification, as long as
                              # we only have dots and alphanumeric characters. More for lining up^
            raise ValueError('Domains are expected to contain dots (.), read \'{domain}\'.'.format(domain=domain))

        self.domain = domain
        self.searchString = ""
        domainParts = domain.split('.')
        for i in range(len(domainParts),0,-1):
            if not domainParts[i-1].replace('-', '').isalnum(): # Not the prettiest or most strictly accurate way of doing this,
                                                                # but will be sufficient for our purposes.
                raise ValueError('Domains can only contain alphanumeric characters, hyphens, and dots, read \'{domain}\'.'.format(domain=domain))
            self.searchString += domainParts[i-1]
            if i > 1:
                self.searchString += ','
        self.loadHistory()
        Domain.domains.append(self)

    def __repr__(self):
        return self.domain

    def __eq__(self, other):
        return str(self) == str(other)

    def loadHistory(self):
        logger.debug('Loading history for %s', self.domain)

        p = Path('history/' + self.domain)

        if path_is_safe(p, self) and p.exists():
            with p.open('r') as f:
                self.history = json.load(f)
                logger.debug('Loaded search history for %s', self.domain)
        else:
            self.history = {}

    def updateHistory(self, archiveID, key, history):
        logger.debug('Updating history for %s (%s: %s)', self.domain, archiveID, str(history))
        if not archiveID in self.history:
            self.history[archiveID] = {'completed': 0, 'failed': 0, 'results': 0}
        self.history[archiveID][key] = history
        p = Path('history', self.domain)
        if path_is_safe(p, self):
            if not p.parents[0].exists():
                p.parents[0].mkdir()
            with p.open('w') as f:
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
        if not archive.clusterIndex: # Implies indexPathsURI is also empty
            archive.updatePaths()
        for line in archive.clusterIndex.read().splitlines():
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

        # This search format should mean we're always left of anything matching our search string.
        position = bisect.bisect_left(index, (self.searchString + '(', 0, "", 0, 0, 0))
        # We may (and likely will) have matches in the index cluster prior to our match.
        results.append(index[position-1])
        while position < len(index):
            if is_match(index[position][0], self.searchString):
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
        for cluster in clusters:
            # We do not need a call to Archive.updatePaths() here, we should only get here after Domain.search()
            index = []
            if config.cache_index_clusters:
                cacheFileName = '.cache/' + archive.archiveID + '/' + cluster[2] + '-' + str(cluster[5])
            else:
                cacheFileName = None
            indexFile = RemoteFile(
                config.archive_host + '/' + archive.indexPathsURI + cluster[2],
                cacheFileName,
                cluster[3],
                cluster[4])
            for line in indexFile.read().splitlines():
                searchable_string,timestamp,json = line.split(' ', 2)
                index.append((searchable_string, int(timestamp), json))
            position = bisect.bisect_left(index, (self.searchString, 0, ""))
            # Unlike the cluster index, there should be no earlier result than position.
            while position < len(index):
                if is_match(index[position][0], self.searchString):
                    # Only the json data will be interesting from here on.
                    results.append(index[position][2])
                    position += 1
                else:
                    break
        self.memoizeCache['searchClusters'] = (self, archive, results)
        if len(results) == 0:
            self.updateHistory(archive.archiveID, 'completed', 0)
        self.updateHistory(archive.archiveID, 'results', len(results))
        logger.debug('Found %d search results for %s/%s.', len(results), self.domain, archive.archiveID)
        return results

    def getFile(self, archive, index):
        # First, determine what to fetch.
        if archive.archiveID not in self.history:
            position = 0
        elif type(self.history[archive.archiveID]['completed']) == int:
            position = self.history[archive.archiveID]['completed']

        logger.debug('Result found at %d', position)

        logger.debug('Loading JSON: %s', index[position])
        # Everything is treated as strings, so we will need to convert integers.
        fileInfo = json.loads(index[position])

        if int(fileInfo['length']) > config.max_file_size:
            logger.warning('Skipping download of %s as file exceeds size limit at %s bytes.', fileInfo['filename'], fileInfo['length'])
            self.updateHistory(archive.archiveID, 'completed', position+1)
        else:
            filerange = '-' + fileInfo['offset'] + '-' + str(int(fileInfo['offset'])+int(fileInfo['length'])-1)

            filename = config.pywb_collection_dir + '/'
            if fileInfo['filename'].endswith('.arc.gz'):
                for name in fileInfo['filename'].split('/'):
                    filename += name
                    filename = filename[0:len(filename)-7] + filerange + '.arc.gz'
            elif fileInfo['filename'].endswith('.warc.gz'):
                _,_,_,partial_path,_,warcfile = fileInfo['filename'].split('/')
                filename += partial_path + '-' + warcfile[0:len(warcfile)-8] + filerange + '.warc.gz'

            url = config.archive_host + '/' + fileInfo['filename']
            rf = RemoteFile(url, filename, int(fileInfo['offset']), int(fileInfo['length']), self.domain, archive.archiveID)
            logger.debug('Downloading from %s (range %i-%i) to %s', url, int(fileInfo['offset']), int(fileInfo['offset'])+int(fileInfo['length'])-1, filename)
            try:
                rf.download()
            except (requests.RequestException, BadHTTPStatus):
                raise
            finally:
                self.updateHistory(archive.archiveID, 'completed', position+1)

#

def main():
    logger.info('Collector running.')
    archives = Archives()
    domains = []

    logger.debug('Loading retry queue.')
    retryqueue = RetryQueue()
    retryqueue.load()

    failcounter = 0
    domains_last_modified = 0

    finished_message = False

    while True:
        if Path(config.domain_list_file).stat().st_mtime > domains_last_modified:
            if domains_last_modified == 0:
                logger.info('Reading domain list.')
            else:
                logger.info('Domain list changed, reloading.')

            # The cheapest way would be to clear the lists we have, then rebuild them.
            # It also ensures we will honor the order they are listed in, should it have changed,
            # without any extra steps. It will, however, lack in elegance. It also means we'll have
            # to reload any history we have saved.
            # If I get spare time, I may rewrite this.

            domains = []
            Domain.domains = [] # This bit is ugly.

            with config.domain_list_file.open('r') as f:
                line_number = 0
                for line in f.read().splitlines():
                    if len(line) == 0:
                        logger.debug('Empty line in {dconf}, skipping.'.format(dconf=config.domain_list_file))
                        break
                    line_number += 1
                    if line in domains:
                        logger.warning('Duplicate domain: %s (line %d in %s)', line, line_number, str(config.domain_list_file))
                    else:
                        domains.append(Domain(line))

            domains_last_modified = Path(config.domain_list_file).stat().st_mtime

        if failcounter > 0 and failcounter % (config.critical_warn_interval * 60) == 0:
            #TODO: Do we at all want to keep running here? Also, should we use different intervals? Should it be configurable?
            logger.critical('Unable to retrieve data for the past %d hours, please see error log for details.', int(failcounter/60))
        try:
            archives.update()
        except (requests.RequestException, BadHTTPStatus):
            failcounter += 1
            time.sleep(60)
            continue

        archive = None
        domain = None
        for d in domains:
            # Not the most elegant solution, but we'll want a double break somehow.
            if domain:
                break
            for _,a in archives:
                # 1 will equal True, so instead, we will have to do a type comparison.
                if not a.archiveID in d.history or d.history[a.archiveID]['completed'] < d.history[a.archiveID]['results']:
                    domain = d
                    archive = a
                    break

        if not domain:
            if not finished_message:
                logger.info('All searches currently finished, next archive list update check in %.2f seconds.', 86400 - (time.time() - archives.lastUpdate))
                finished_message = True
                time.sleep(10)
            continue

        finished_message = False

        try:
            results = domain.search(archive)
        except (requests.RequestException, BadHTTPStatus):
            failcounter += 1
            time.sleep(60)
            continue
        if len(results) > 0:
            results = domain.searchClusters(archive, results)
            if len(results) > 0:
                try:
                    domain.getFile(archive, results)
                except (requests.RequestException, BadHTTPStatus):
                    failcounter += 1
                    time.sleep(60)
                    continue

        try:
            retryqueue.process()
        except (requests.RequestException, BadHTTPStatus):
            failcounter += 1
            time.sleep(60)
            continue

        # If we get this far, this cycle has been successful. Reset fail counter.
        failcounter = 0

if __name__ == "__main__":
    main()
