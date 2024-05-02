# Common Crawl Collector
The purpose of this script is to search the [Common Crawl](https://commoncrawl.org) archives for specific domains, and download their matching content, for use with [pywb](https://pywb.readthedocs.io/). It has been written by Jens Nilsson Sahlin to aid research at Linköping University.

## Usage
The script is intended to run as a daemon over a long period of time. Other uses may be possible, but would not be advised without proper modifications.

When running alongside pywb, it is assumed that pywb has been configured to use automatic index updating. The purpose is twofold: stay out of pywb's venv, and don't trigger index rebuilds too frequently.

A few basic configuration options are housed at the beginning of the script. I will choose not to describe them here for the time being, as they should hopefully do that well enough themselves.

The domains to be searched should be listed in [domains.txt](domains.txt), which will be expected to be a plaintext file. Separate domain entries with a newline. By request, the script will prioritise finish searching all archives for one domain, letting the domain's history be completed. Only domains will be accepted, and will implicitly include all subdomains. Full URLs are not supported, because when combined with the implicit inclusion of subdomains, this would be less optimal.

### status.py
`status.py` can be used to get completion information on individual domains. Usage is simple: `status.py <domain>`. 

## Exceptions
The script comes with two custom exceptions. Both inherit from Exception with no additional changes. The exceptions are `ParserError` and `BadHTTPStatus`.

### ParserError
`ParserError` is the most important to take note of. It will be raised if we are unable to parse output of either the main index listing, or the cc-index.paths file, and should not be handled. This _could_ trigger if we get bad data from the server, but it will most likely mean there has been a format change, and our parsing algorithm will need to be updated accordingly.

### BadHTTPStatus
Will be raised if we get a HTTP response other than 200 or 206. Arguments passed, in order, are: url, offset, length, status code, and status message. Presently have handlers in relevant locations, and should not have any loose chains.

## License
Licensed under [GPLv3](https://www.gnu.org/licenses/gpl-3.0.html), see [license.txt](license.txt)