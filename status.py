#!/usr/bin/env python3
# Status reporter for the Common Crawl Collector
# Written by Jens Nilsson Sahlin for Linköping University 2024
# Additional details will be available in README.md
#

import json
from pathlib import Path
import sys

def get_status(domain):
    history_file = Path('history/', domain)
    if not history_file.exists():
        return
    with history_file.open('r') as f:
        history = json.load(f)
    completed_archives = 0
    results = {}
    for archive,hist in history.items():
        if hist['results'] == 0 or hist['completed'] == hist['results'] and hist['failed'] == 0:
            completed_archives += 1
        else:
            results[archive] = {
                'completed' : hist['completed'] - hist['failed'],
                'results' : hist['results'],
                'failed' : hist['failed']
            }
    return {
        'completed_archives' : completed_archives,
        'partial' : len(history)-completed_archives,
        'per_archive' : results
    }

def main():
    if not Path('status.py').exists():
        print('status.py should be run from the directory where it is located.')
        return
    if len(sys.argv) != 2: # TODO: Use no arguments for 'all' instead.
        sys.exit('Usage: ' + sys.argv[0] + ' <domain|all>')
    with Path('archive_count').open('r') as f:
        archive_count = int(f.read())
    if sys.argv[1] == 'all':
        if not Path('domains.conf').exists(): # This could potentially have been reconfigured, though.
            print('domains.conf not found')   # But pretend otherwise for now.
            return
        with Path('domains.conf').open('r') as f:
            domains = {
                'completed' : 0,
                'partial' : 0,
                'total' : 0,
            }
            archives = {
                'completed' : 0,
                'partial' : 0,
            }
            partial_list = []
            for domain in f.read().splitlines():
                domains['total'] += 1
                status = get_status(domain)
                if status:
                    archives['completed'] += status['completed_archives']
                    archives['partial'] += status['partial']
                    if status['completed_archives'] > 0 or status['partial'] > 0:
                        if status['completed_archives'] == archive_count:
                            domains['completed'] += 1
                        else:
                            domains['partial'] += 1
                            partial_list.append(domain + ' (' + str(status['completed_archives']) + '/' + str(archive_count) + ')')
            print('{completed}/{total} domains have been fully processed, and {partial} have been partially processed.'.format(
                completed = domains['completed'],
                partial = domains['partial'],
                total = domains['total'])
            )
            # TODO: Very bad wording for the partial bit.
            print('An average of {average:.1f}/{archives} archives have been fully processed for each domain. {partial} archives have been partially processed.'.format(
                average = archives['completed'] / domains['total'],
                archives = archive_count,
                partial = archives['partial'])
            )
            if len(partial_list) > 0:
                print('Partially processed domains (completed/total archives): {partial}'.format(
                    partial = ', '.join(partial_list))
                )

    else:
        status = get_status(sys.argv[1])
        if not status:
            print('No history found for ' + sys.argv[1])
            return
        print('{domain} has been fully processed in {completed}/{total} archives (+{partial} in progress).'.format(
            domain = sys.argv[1],
            completed = status['completed_archives'],
            partial = status['partial'],
            total = archive_count)
        )
        for archive,stat in status['per_archive'].items():
            print('{archive}: {completed}/{results} ({percentage:.1f}%) results downloaded, {failed} failed.'.format(
                archive = archive,
                completed = stat['completed'],
                results = stat['results'],
                percentage = 100*stat['completed']/stat['results'],
                failed = stat['failed'])
            )

if __name__ == '__main__':
    main()
