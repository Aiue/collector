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
import logging.handlers
import os
from pathlib import Path
import requests
import time

try:
    from prometheus_client import start_http_server, Gauge, Counter, Enum, Info, Summary
except ModuleNotFoundError:
    # Create dummy references instead of the above.
    def start_http_server(port):
        pass

    class Counter:
        def __init__(*args):
            pass
        def inc(*args):
            pass

    class Gauge:
        def __init__(*args):
            pass
        def inc(*args):
            pass
        def dec(*args):
            pass
        def set(*args):
            pass
        def set_function(*args):
            pass

    class Enum:
        def __init__(*args, **kwargs):
            pass
        def state(*args):
            pass

    class Info:
        def __init__(*args):
            pass
        def info(*args):
            pass

    class Summary:
        def __init__(*args):
            pass
        def observe(*args):
            pass

# I don't like the configuration file alternatives python offers. I'll write my own.
class Config:
    # Set defaults
    archive_host = 'https://data.commoncrawl.org'
    archive_list_uri = '/cc-index/collections/index.html'
    max_file_size = 104857600 # Max file size we'll download.
                              # Currently set to 100 MiB, which may seem ridiculously large in context.
                              # Only applies to [W]ARC files.
    min_request_interval = 1.0
    max_request_interval = 60.0
    cache_index_clusters = False
    pywb_collection_dir = 'path/to/pywb/collection' # Should (probably) also be Pathified.
                                                    # However, this would require more extensive rewrites.
    domain_list_file = Path('domains.conf')
    safe_path = Path.cwd()
    prometheus_port = 1234
    cache_dir = Path('.cache')
    notification_email = None
    mail_from_address = None

    def __init__(self, configFile):
        if configFile.exists():
            with configFile.open('r') as f:
                for line in f.read().splitlines():
                    # This isn't pretty, but it will ensure the preferred format is viable.
                    key,value = line.split('=')
                    if key == 'cache_index_clusters':
                        value = bool(value)
                    elif key == 'min_request_interval':
                        value = float(value)
                    elif key in ['domain_list_file', 'safe_path', 'cache_dir']:
                        value = Path(value)
                    elif key in ['max_file_size', 'prometheus_port']:
                        value = int(value)
                    elif key not in ['archive_host', 'archive_list_uri', 'mail_from_address', 'notification_email', 'pywb_collection_dir']:
                        raise RuntimeError('Unknown configuration key: %s' % key)
                    setattr(self, key, value)

config = Config(Path('collector.conf'))

# Init loggers
logger = logging.getLogger()
logging.config.fileConfig('logger.conf')
mailer = logging.getLogger('mailer')
mailer.setLevel('INFO')
if config.notification_email == None or config.mail_from_address == None:
    mailer.addHandler(logging.NullHandler())
else:
    mailer.addHandler(logging.handlers.SMTPHandler('localhost', config.mail_from_address, [config.notification_email], 'Collector Status Update'))
mailer.propagate = False

# Exceptions
class ParserError(Exception):
    pass

class BadHTTPStatus(Exception):
    pass

# Utility functions
def human_readable(b):
    for prefix in ('', 'Ki', 'Mi', 'Gi', 'Ti'): # Doubt we'll need even all of these.
        if abs(b) < 1024:
            if prefix == '':
                return '%d B' % int(b) # Just a special case to not get a float for bytes. Ensure b is treated as integer.
            return '%.1f %sB' % (b, prefix)
        b /= 1024.0
    return '%.1f PiB' % b # Fallback. If we get this: worry.

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
             or str(path).startswith(str(config.cache_dir))
    )):
        msg = 'Unsafe path: %s' % self
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
class Monitor:
    monitors = {}
    status_cache = {
        'current_archive':'N/A',
        'current_domain':'N/A',
        'current_progress':'N/A',
        'latest_archive':'N/A',
    }
    def __init__(self, monitor):
        self.monitors[monitor] = self
        self.retryqueue = Gauge('collector_retryqueue', 'Retry Queue Entries')
        self.requests = Counter('collector_requests', 'Requests Sent')
        self.failed = Counter('collector_failed', 'Failed Requests')
        self.state = Enum('collector_state', 'Current State', states=['collecting', 'idle'])
        self.status = Info('collector_status', 'Collector Status Information')
        self.download_size = Summary('collector_download_size', 'Download Size')

    def get(name):
        if name in Monitor.monitors: return Monitor.monitors[name]
        return Monitor(name)

    def UpdateStatus(self, **kwargs):
        for k,v in kwargs.items():
            if k not in self.status_cache:
                logger.warning('Unknown status type: %s' % k)
                continue
            self.status_cache[k] = v
        self.status.info(self.status_cache)


class Archive:
    def __init__(self, archiveID, indexPathsFile):
        self.archiveID = archiveID
        self.indexPathsFile = RemoteFile(config.archive_host + indexPathsFile)
        self.clusterIndex = None
        self.order = None

    def __repr__(self):
        return self.archiveID

    def updatePaths(self):
        logger.debug('Updating paths for %s.', self.archiveID)
        f = RemoteFile(self.indexPathsFile)
        contents = f.read()
        for line in contents.splitlines():
            if line.endswith('cluster.idx'):
                self.clusterIndex = RemoteFile(config.archive_host + '/' + line, str(config.cache_dir) + '/' + self.archiveID + '/cluster.idx')
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
            if tag == 'tbody':
                self.isScanningTableBody = True
            if self.isScanningTableBody:
                if tag == 'tr':
                    self.archiveID = None
                    self.tdCount = 0

                if tag == 'td':
                    self.tdCount += 1

                if tag == 'a':
                    if self.tdCount == 4:
                        # As of writing, only 'href' attributes are present.
                        # But in case this changes in the future, let's be vigilant.
                        for attribute in attributes:
                            if attribute[0] == 'href':
                                self.archives.append(Archive(self.archiveID, attribute[1]))

        def handle_endtag(self, tag):
            if tag == 'tbody':
                self.isScanningTableBody = False
                
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
        preArchiveCount = 0
        if Path('archive_count').exists():
            with Path('archive_count').open('r') as f:
                preArchiveCount = int(f.read())

        for archive in parser.archives:
            if archive.archiveID not in self.archives:
                monitor = Monitor.get('monitor')
                if not initial:
                    monitor.UpdateStatus(latest_archive=archive.archiveID)
                    logger.info('New archive: %s' % archive.archiveID)
                    mailer.info('New archive: %s' % archive.archiveID)
                elif len(self.archives) == 0:
                    monitor.UpdateStatus(latest_archive=archive.archiveID)
                    if len(parser.archives) > preArchiveCount:
                        mailer.info('New archive: %s' % archive.archiveID)
                self.archives[archive.archiveID] = archive
                self.archives[archive.archiveID].order = len(self.archives)
                if len(self.archives) > preArchiveCount:
                    with Path('archive_count').open('w') as f:
                        f.write(str(len(self.archives)))

        parser.close()
        self.lastUpdate = time.time()
        if initial:
            logger.info('Found %d archives.', len(self.archives))

class RemoteFile:
    requests = { # Using a dict for reference retention.
        'last': 0,
        'failed': 0,
    }

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
        else:
            self.write(contents)

    def read(self):
        #logger.debug('Reading from %s', self.url)
        contents = None
        if self.filename and self.filename.exists(): # File is in cache.
            if self.length:
                size = self.length
            else:
                r = requests.head(self.url)
                size = int(r.headers['Content-Length'])

            fsize = self.filename.stat().st_size
            if fsize == size:
                #logger.debug('File is cached, reading from %s', self.filename)
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
        #logger.debug('Writing from %s to %s', self.url, self.filename)
        if not self.filename.parents[0].exists():
            logger.info('Recursively creating directory \'%s\'.', self.filename.parents[0])
            self.filename.parents[0].mkdir(parents=True)
        with self.filename.open('wb') as f:
            f.write(contents)

    def get(self):
        #logger.debug('Getting from %s', self.url)
        time_diff = time.time() - self.requests['last']
        if (time_diff < config.min_request_interval):
            logger.debug('Request limit reached, sleeping for %f seconds.', time_diff)
            time.sleep(time_diff)

        time_start = time.time()
        headers = None # Should not need to be initialized/emptied, but do it anyway.
        if type(self.offset) == int and self.length:
            headers = {'Range': 'bytes=' + str(self.offset) + '-' + str(self.offset+self.length-1)}
        self.requests['last'] = time.time()
        monitor = Monitor.get('monitor')
        monitor.requests.inc()
        try:
            r = requests.get(self.url, headers=headers)
        except requests.RequestException as error:
            monitor.failed.inc()
            logger.error('Could not get %s - %s', self.url, error)
            raise
        finally:
            monitor = Monitor.get('monitor')
            download_size = self.length if self.length else int(r.headers['Content-Length']) if 'Content-Length' in r.headers else 0
            monitor.download_size.observe(download_size)
            logger.debug('Downloaded %d bytes in %f seconds. (%s/s)' % (download_size, time.time() - time_start), human_readable(download_size/(time.time()-time_start)))
        if not (r.status_code >= 200 and r.status_code < 300):
            # This could imply a problem with parsing, raise it as such rather than simply bad status.
            if r.status_code >= 400 and r.status_code < 500:
                raise ParserError('HTTP response %d indicates a potential parsing issue. This should be investigated.', r.status_code)
            monitor.failed.inc()
            sleep = config.min_request_interval * pow(1.5, self.requests['failed'])
            if sleep > config.max_request_interval:
                sleep = config.max_request_interval
            self.requests['failed'] += 1
            logger.error('Bad HTTP response %d %s for %s, sleeping for %.2f seconds (fail counter=%d).', r.status_code, r.reason, self.url, sleep, self.requests['failed'])
            time.sleep(sleep)
            raise BadHTTPStatus(self.url, self.offset, self.length, r.status_code, r.reason)

        self.requests['failed'] = 0
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
    domains = []
    
    def __init__(self, domain): # TODO: Check that it's not a duplicate.
        logger.debug('New domain: %s', domain)
        if not '.' in domain: # Additional validation will follow when building the search string.
                              # We don't need to be super strict with the verification, as long as
                              # we only have dots and alphanumeric characters. More for lining up^
            raise ValueError('Domains are expected to contain dots (.), read \'%s\'.' % domain)

        self.domain = domain
        self.searchString = ''
        domainParts = domain.split('.')
        for i in range(len(domainParts),0,-1):
            if not domainParts[i-1].replace('-', '').isalnum(): # Not the prettiest or most strictly accurate way of doing this,
                                                                # but will be sufficient for our purposes.
                raise ValueError('Domains can only contain alphanumeric characters, hyphens, and dots, read \'%s\'.' % domain)
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
        #logger.debug('Updating history for %s/%s (%s: %s)', self.domain, archiveID, key, str(history))
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

class Search:
    def __init__(self, domain, archive):
        self.domain = domain
        self.archive = archive
        self.clusters = None
        self.archives = None # A bit misleading. An archive in this case refers to an ARC or WARC,
                             # which is distinctly different from a full archive release.
                             # CC semantics can be a bit peculiar at times.

    def process(self):
        if not self.clusters:
            self.findClusters()
        if not self.archives:
            self.findArchives()
        if len(self.archives) > 0:
            self.getFile()

    def findClusters(self):
        logger.info('Processing %s in %s.', self.domain.domain, self.archive.archiveID)

        self.clusters = []
        index = []
        if not self.archive.clusterIndex: # Implies indexPathsURI is also empty
            self.archive.updatePaths()
        for line in self.archive.clusterIndex.read().splitlines():
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
        position = bisect.bisect_left(index, (self.domain.searchString + ')', 0, '', 0, 0, 0))
        logger.debug('(cluster index) Potential match at line %d out of %d. (Between %s and %s)', position+1, len(index), (position <= 0 and '(index out of range)' or index[position-1][0]), index[position][0])
        # We may (and likely will) have matches in the index cluster prior to our match.
        self.clusters.append(index[position-1])
        while position < len(index):
            if is_match(index[position][0], self.domain.searchString):
                self.clusters.append(index[position])
                position += 1
            else:
                break

    def findArchives(self): # TODO: Not happy with variable names here. Need to revisit and rename.
        logger.debug('Searching %s clusters for %s', self.archive.archiveID, self.domain.domain)

        self.archives = []
        for cluster in self.clusters:
            index = []
            if config.cache_index_clusters:
                cacheFileName = str(config.cache_dir) + '/' + self.archive.archiveID + '/' + cluster[2] + '-' + str(cluster[5])
            else:
                cacheFileName = None
            indexFile = RemoteFile(
                config.archive_host + '/' + self.archive.indexPathsURI + cluster[2],
                cacheFileName,
                cluster[3],
                cluster[4])
            for line in indexFile.read().splitlines():
                searchable_string,timestamp,json = line.split(' ', 2)
                index.append((searchable_string, int(timestamp), json))

            if cluster is self.clusters[0]:
                position = bisect.bisect_left(index, (self.domain.searchString, 0, ''))
            else:
                position = 0
            logger.debug('Index insertion point at line %d out of %d. (%s)', position+1, len(index), index[position][0])            
            # Unlike the cluster index, there should be no earlier result than position.
            while position < len(index):
                if is_match(index[position][0], self.domain.searchString):
                    # Only the json data will be interesting from here on.
                    self.archives.append(index[position][2])
                    position += 1
                else:
                    break
        if len(self.archives) == 0:
            self.domain.updateHistory(self.archive.archiveID, 'completed', 0)
        self.domain.updateHistory(self.archive.archiveID, 'results', len(self.archives))
        logger.info('Found %d search results.', len(self.archives))

    def getFile(self):
        # First, determine what to fetch.
        if self.archive.archiveID not in self.domain.history:
            position = 0
        elif type(self.domain.history[self.archive.archiveID]['completed']) == int:
            position = self.domain.history[self.archive.archiveID]['completed']

        fileInfo = json.loads(self.archives[position])

        monitor = Monitor.get('monitor')
        monitor.UpdateStatus(current_progress='%d/%d (%d%%)' % (position + 1, self.domain.history[self.archive.archiveID]['results'], (100*(position + 1) / self.domain.history[self.archive.archiveID]['results'])))
        if int(fileInfo['length']) > config.max_file_size:
            logger.warning('Skipping download of %s as file exceeds size limit at %s bytes.', fileInfo['filename'], fileInfo['length'])
            self.domain.updateHistory(self.archive.archiveID, 'completed', position+1)
        else:
            filerange = '-' + fileInfo['offset'] + '-' + str(int(fileInfo['offset'])+int(fileInfo['length'])-1)

            filename = config.pywb_collection_dir + '/'
            if fileInfo['filename'].endswith('.arc.gz'):
                filename += fileInfo['filename'].replace('/', '-')
                filename = filename[0:len(filename)-7] + filerange + '.arc.gz'
            elif fileInfo['filename'].endswith('.warc.gz'):
                _,_,_,partial_path,_,warcfile = fileInfo['filename'].split('/')
                filename += partial_path + '-' + warcfile[0:len(warcfile)-8] + filerange + '.warc.gz'
            else:
                raise RuntimeError('Unknown file ending for %s', fileInfo['filename'])

            url = config.archive_host + '/' + fileInfo['filename']
            rf = RemoteFile(url, filename, int(fileInfo['offset']), int(fileInfo['length']), self.domain.domain, self.archive.archiveID)
            #logger.debug('Downloading from %s (range %i-%i) to %s', url, int(fileInfo['offset']), int(fileInfo['offset'])+int(fileInfo['length'])-1, filename)
            try:
                rf.download()
            except (requests.RequestException, BadHTTPStatus):
                raise
            finally:
                self.domain.updateHistory(self.archive.archiveID, 'completed', position+1)

#

def main():
    logger.info('Collector running.')
    archives = Archives()
    domains = []
    domains_last_modified = 0
    finished_message = False
    monitor = Monitor.get('monitor')
    monitor.state.state('idle')
    hasProcessed = False
    current_search = None

    logger.debug('Loading retry queue.')
    retryqueue = RetryQueue()
    retryqueue.load()

    start_http_server(config.prometheus_port)

    while True:
        monitor.retryqueue.set(len(retryqueue.queue))
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
                    line_number += 1
                    if len(line) == 0:
                        logger.debug('Empty line in %s, skipping.', config.domain_list_file)
                        break
                    if line in domains:
                        logger.warning('Duplicate domain: %s (line %d in %s)', line, line_number, str(config.domain_list_file))
                    else:
                        domains.append(Domain(line))

            domains_last_modified = Path(config.domain_list_file).stat().st_mtime

        archives.update()

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
                    monitor.UpdateStatus(current_domain=str(domain), current_archive='%s (%d/%d)' % (archive.archiveID, archive.order, len(archives.archives)))
                    break

        retryqueue.process()

        if not domain:
            current_search = None # Make sure we're not sitting on memory we don't need.
            monitor.state.state('idle')
            if not finished_message:
                monitor.UpdateStatus(current_domain='N/A', current_archive='N/A', current_progress='N/A')
                logger.info('All searches currently finished, next archive list update check in %.2f seconds.', 86400 - (time.time() - archives.lastUpdate))
                finished_message = True
            if hasProcessed:
                mailer.info('All configured domains have been processed in all current archives.%s' % ('\n%d items remain in retry queue.' % len(retryqueue.queue) if len(retryqueue.queue) > 0 else ''))
                hasProcessed = False
            time.sleep(10)
            continue

        monitor.state.state('collecting')
        finished_message = False

        if not current_search or current_search.domain != domain or current_search.archive != archive:
            current_search = Search(domain, archive)
        try:
            current_search.process()
        except (requests.RequestException, BadHTTPStatus) as error:
            if isinstance(error, BadHTTPStatus):
                logger.warning('Could not retrieve %s: %d %s' % error[0], error[3], error[4])
            else:
                logger.warning(error)

        hasProcessed = True

if __name__ == '__main__':
    main()
