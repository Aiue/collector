#!/usr/bin/env python3
# Status reporter for the Common Crawl Collector
# Written by Jens Nilsson Sahlin for Linköping University 2024
# Additional details will be available in README.md
#

import json
from pathlib import Path
import sys

def main():
    if len(sys.argv) != 2:
        print('Usage: ' + sys.argv[0] + ' <domain>')
        return
    domain = sys.argv[1]
    history_file = Path('history/', domain)
    if not history_file.exists():
        print('No history found for ' + domain)
        return
    with history_file.open('r') as f:
        history = json.load(f)
    with Path('archive_count').open('r') as f:
        archive_count = int(f.read())
    completed_archives = 0
    output = ""
    for archive,hist in history.items():
        if hist['results'] == 0 or hist['completed'] == hist['results'] and hist['failed'] != 0:
            completed_archives += 1
        else:
            completed = hist['completed'] - hist['failed']
            output += '{archive}: {completed}/{results} ({percentage:.1f}%) results downloaded, {failed} failed.\n'.format(
                archive = archive,
                completed=hist['completed'] - hist['failed'],
                results = hist['results'],
                percentage = 100*(hist['completed'] - hist['failed']) / hist['results'],
                failed = hist['failed'])

    print('{domain} has been fully processed in {completed} archives and partially processed in {partial}, out of {total} archives in total.'.format(
        domain=domain,
        completed = completed_archives,
        partial=len(history)-completed_archives,
        total=archive_count))
    if len(output) > 0:
        print(output)

if __name__ == "__main__":
    main()
