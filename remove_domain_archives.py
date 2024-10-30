#!/usr/bin/env python3
# remove_domain_archive.py <domain>
# Removes downloaded archives for specified domain.
# Relies on these being indexed on pywb.
# Recommend stopping pywb before running this.

import bisect
from collector import Config, is_match
import json
from pathlib import Path
import sys

config = Config(Path('collector.conf'))

def main():
    if len(sys.argv) != 2:
        sys.exit('Usage: ' + sys.argv[0] + ' <domain>')
    indexFile = Path(Path(config.download_dir).parents[0], 'indexes', 'autoindex.cdxj')
    index = []
    with indexFile.open('r') as f:
        for line in f.read().splitlines():
            searchable_string,timestamp,info = line.split(' ', 2)
            index.append((searchable_string, int(timestamp), info))

    if len(index) == 0:
        print('Nothing to remove.')
        sys.exit()

    domainParts = sys.argv[1].split('.')
    searchString = ''
    for i in range(len(domainParts),0,-1):
        if not domainParts[i-1].replace('-', '').isalnum(): # Not the prettiest or most strictly accurate way of doing this,
                                                            # but will be sufficient for our purposes.
            raise ValueError('Domains can only contain alphanumeric characters, hyphens, and dots, read \'%s\'.' % sys.argv[1])
        searchString += domainParts[i-1]
        if i > 1:
            searchString += ','

    position = bisect.bisect_left(index, (searchString, 0, ''))
    results = 0
    if position == len(index):
        position -= 1
    while is_match(index[position][0], searchString):
        results += 1
        info = json.loads(index.pop(position)[2])
        try:
            Path(config.download_dir, info['filename']).unlink()
        except FileNotFoundError:
            print('Indexed file %s not found.' % info['filename'])
        if len(index) <= position:
            break

    print('Removed %d files.' % results)

    print('Removing history/%s (if existing)' %  sys.argv[1])
    Path('history', sys.argv[1]).unlink()
    print('Writing new index.')
    with indexFile.open('w') as f:
        for line in index:
            f.write(line[0] + ' ' + str(line[1]) + ' ' + line[2] + '\n')

if __name__ == '__main__':
    main()
