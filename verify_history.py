#!/usr/bin/env python3
# Tool for verifying saved search history matches actual history.
# Only operates on cached data.
# Author: Jens Nilsson Sahlin

import bisect
from collector import is_match, Config, Domain
import json
import logging
import logging.handlers
from pathlib import Path

config = Config(Path('collector.conf'))

logger = logging.getLogger()
email_handler = logging.handlers.SMTPHandler('localhost', 'noreply-common-crawl-collector@ccc-test.it.liu.se', ['jens.nilsson.sahlin@liu.se'], 'CC-Collector Diagnostics Report')
email_handler.setFormatter(logging.Formatter(fmt=None))
logger.addHandler(email_handler)
    
def main():
    # Load domains.
    domains = []
    mismatches = []
    with config.domain_list_file.open('r') as f:
        for line in f.read().splitlines():
            if len(line) == 0 or line in domains:
                break
            else:
                domains.append(Domain(line))

    for cluster_index in Path(config.cache_dir).glob('*/cluster.idx'):
        cindex = []
        with cluster_index.open('r') as f:
            for line in f.read().splitlines():
                searchable_string,rest = line.split(' ')
                timestamp,filename,offset,length,cluster = rest.split('\t')
                cindex.append(
                    (searchable_string, # 0
                     int(timestamp),    # 1
                     filename,          # 2
                     int(offset),       # 3
                     int(length),       # 4
                     int(cluster),      # 5
                    ))

        for domain in domains:
            clusters = []
            matches = 0
            position = bisect.bisect_left(cindex, (domain.searchString + '(', 0, "", 0, 0, 0))
            clusters.append(cindex[position-1])
            while position < len(cindex):
                if is_match(cindex[position][0], domain.searchString):
                    clusters.append(cindex[position])
                    position += 1
                else:
                    break

            for cluster in clusters:
                aindex = []
                cacheFile = Path(cluster_index.parent, cluster[2] + '-' + str(cluster[5]))
                with cacheFile.open('r') as f:
                    for line in f.read().splitlines():
                        searchable_string,timestamp,json = line.split(' ', 2)
                        aindex.append((searchable_string, int(timestamp), json))
                if cluster is clusters[0]:
                    position = bisect.bisect_left(aindex, (domain.searchString, 0, ""))
                else:
                    position = 0
                while position < len(aindex):
                    if is_match(aindex[position][0], domain.searchString):
                        matches += 1
                        position += 1
                    else:
                        break

            # Compare matches to history. If they mismatch, something's broken.
            archiveID = cluster_index.parts[len(cluster_index.parts)-2]
            if not archiveID in domain.history:
                mismatches.append((archiveID, domain, matches, 0, 0))
            elif domain.history[archiveID]['results'] != matches:
                mismatches.append((archiveID, domain, matches, domain.history[archiveID]['results']), 1)

    if len(mismatches) > 0:
        msg = ''
        for mismatch in mismatches:
            # 0 archiveID
            # 1 domain
            # 2 matches
            # 3 history results
            # 4 flag: 0 for no history, 1 for history but mismatch
            msg += '%s in %s: %d matches, ' % (mismatch[1], mismatch[0], mismatch[2])
            if mismatch[4] == 0:
                msg += 'no recorded history found\n'
            else:
                msg += '%d in history' % mismatch[3]

            if mismatch is not mismatches[len(mismatches)-1]:
                msg += '\n'

        logger.info(msg)
                
    else: # No errors! Yay!
        logger.info('verify_history.py ran, no problems detected.')

if __name__ == "__main__":
    main()
