# Common Crawl Collector
The purpose of this script is to search the [Common Crawl](https://commoncrawl.org) archives for specific domains, and download their matching content, for use with [pywb](https://pywb.readthedocs.io/). It has been written by Jens Nilsson Sahlin to aid research at Linköping University.

## Usage
The script is intended to run as a daemon over a long period of time. Other uses may be possible, but would not be advised without proper modifications.

A few basic configuration options are housed at the beginning of the script. I will choose not to describe them here for the time being, as they should hopefully do that well enough themselves.

The domains to be searched should be listed in [domains.txt](domains.txt), which will be expected to be a plaintext file. Separate domain entries with a newline. By request, the script will prioritise finish searching all archives for one domain, letting the domain's history be completed.

## License
Licensed under [GPLv3](https://www.gnu.org/licenses/gpl-3.0.html), see [license.txt](license.txt)