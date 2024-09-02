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
        print('Usage: ' + sys.argv[0] + ' <domain>')
        return
    indexFile = Path(Path(config.pywb_collection_dir).parents[0], 'root', 'autoindex.cdxj')
    index = []
    with indexFile.open('r') as f:
        for line in f.read.splitlines():
            searchable_string,timestamp,json = line.split(' ', 2)
            index.append((searchable_string, int(timestamp), json))

    domainParts = sys.argv[1].split('.')
    searchString = ""
    for i in range(len(domainParts),0,-1):
        if not domainParts[i-1].replace('-', '').isalnum(): # Not the prettiest or most strictly accurate way of doing this,
                                                            # but will be sufficient for our purposes.
            raise ValueError('Domains can only contain alphanumeric characters, hyphens, and dots, read \'%s\'.' % sys.argv[1])
        searchString += domainParts[i-1]
        if i > 1:
            searchString += ','

    position = bisect.bisect_left(index, (searchString, 0, ''))
    results = 0
    while is_match(index[position], searchString):
        reults += 1
        info = json.loads(index.pop(position)[2])
        try:
            Path(config.pywb_collection_dir, info['filename']).unlink()
        except FileNotFoundError:
            print('Indexed file %s not found.' % info['filename'])

    print('Removed %d files.' % results)

    print('Removing history/%s (if existing)' %  domain)
    Path('history', domain).unlink(missing_ok=True)
    print('Writing new index.')
    with indexFile.open('w') as f:
        for line in index:
            f.write(line[0] + ' ' + line[1] + ' ' + line[2])

if __name__ == '__main__':
    main()
