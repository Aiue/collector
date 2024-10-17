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
    print('Comparing against pywb index...')
    with Path(Path(config.pywb_collection_dir).parents[0], 'indexes', 'autoindex.cdxj').open('r') as f:
        lineno = 0
        for line in f.read().splitlines():
            lineno += 1
            _,_,info = line.split(' ', 2)
            filename = json.loads(info)['filename']
            if filename in archives: # Not exactly the most optimal approach, but this tool will only be used sparingly.
                archives.remove(filename)
            else:
                missing_archives.append(filename)
            print('\033[FComparing against pywb index... %d' % lineno)
        print('\033[FComparing against pywb index... done')

    print('%d files missing from index' % len(archives), end='')
    if len(archives) > 0:
        with Path('unindexed_files').open('w') as f:
            for archive in archives:
                f.write(archive)
        print(', full list in file \'unindexed_files\'.')
    print('.')

    if len(missing_archives) > 0:
        with Path('missing_files').open('w') as f:
            for archive in missing_archives:
                f.write(archive)
        print('\n%d index entries has no corresponding archive file, full list in file \'missing_files\'.' % len(missing_archives))

if __name__ == '__main__':
    main()
