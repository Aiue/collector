#!/usr/bin/env python3

from collector import Config
import json
from pathlib import Path

config = Config(Path('collector.conf'))

def main():
    print('Building file list... ', end='', flush=True)
    archives = []
    missing_archives = []
    for archive in Path(config.pywb_collection_dir).iterdir():
        archives.append(archive.name)
    print('%d files found.' % len(archives))
    print('Comparing against pywb index... ', end='', flush=True)
    with Path(Path(config.pywb_collection_dir).parents[0], 'indexes', 'autoindex.cdxj').open('r') as f:
        for line in f.read().splitlines():
            _,_,info = line.split(' ', 2)
            filename = json.loads(info)['filename']
            if filename in archives:
                archives.remove(filename)
            else:
                missing_archives.append(filename)

    print('%d files missing from index:' % len(archives))
#    for archive in archives:
#        print(archive)

    if len(missing_archives) > 0:
        print('\nAdditionally, %d index entries has no corresponding archive file:' % len(missing_archives))
#        for archive in missing_archives:
#            print(archive)

if __name__ == '__main__':
    main()
