#!/usr/bin/env python3

from bisect import bisect_left, insort_left
from collector import Config
import json
from pathlib import Path
import sys

config = Config(Path('collector.conf'))

def get_input(msg, valid_inputs):
    print(msg, end='', flush=True)
    key = sys.stdin.read(1)
    print()
    if key in valid_inputs:
        return key
    else:
        print('Unknown key %s, ' % key, end='', flush=True)
        return get_input(msg, valid_inputs)

def main():
    print('Building file list... ')
    archives = []
    missing_archives = []
    for archive in Path(config.pywb_collection_dir).iterdir():
        insort_left(archives, archive.name)
        print('\033[F\033[KBuilding file list... %d' % len(archives))
    print('\033[F\033[KBuilding file list... %d files found.' % len(archives))
    print('Comparing against pywb index...')
    with Path(Path(config.pywb_collection_dir).parents[0], 'indexes', 'autoindex.cdxj').open('r') as f:
        lineno = 0
        for line in f.read().splitlines():
            lineno += 1
            _,_,info = line.split(' ', 2)
            filename = json.loads(info)['filename']
            position = bisect_left(archives,filename)
            if archives[position] == filename:
                archives.pop(position)
            else:
                missing_archives.append(filename)
            print('\033[F\033[KComparing against pywb index... %d' % lineno)
        print('\033[F\033[KComparing against pywb index... %d entries read.' % lineno)

    print('%d files missing from index' % len(archives), end='')
    if len(archives) > 0:
        print('; ', end='')
        key = get_input('[w]rite to file, or [m]ove archives? ', 'mw')
        if key == 'm':
            if not Path('unindexed_files').exists():
                Path('unindexed_files').mkdir()
            elif not Path('unindexed_files').is_dir():
                print('\'unindexed_files\' already exists, but is not a directory.')
                sys.exit()
            for archive in archives:
                Path(config.pywb_collection_dir, archive).rename(Path('unindexed_files', archive))
            print('Files moved to \'unindexed_archives/\'.')
                
        elif key == 'w':
            with Path('unindexed_file_list').open('w') as f:
                for archive in archives:
                    f.write(archive + '\n')
            print('Wrote to \'unindexed_archive_list\'.')
    else:
        print('.')

    if len(missing_archives) > 0:
        with Path('missing_files').open('w') as f:
            for archive in missing_archives:
                f.write(archive + '\n')
        print('%d index entries has no corresponding archive file, full list in file \'missing_files\'.' % len(missing_archives))

if __name__ == '__main__':
    main()
