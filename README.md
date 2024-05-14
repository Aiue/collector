# Common Crawl Collector
The purpose of this script is to search the [Common Crawl](https://commoncrawl.org) archives for specific domains, and download their matching content, for use with [pywb](https://pywb.readthedocs.io/). It has been written by Jens Nilsson Sahlin to aid research at Linköping University.

## Usage
The script is intended to run as a daemon over a long period of time. Other uses may be possible, but would not be advised without proper modifications.

When running alongside pywb, it is assumed that pywb has been configured to use automatic index updating. The purpose is twofold: stay out of pywb's venv, and don't trigger index rebuilds too frequently.

A few basic configuration options are housed at the beginning of the script. I will choose not to describe them here for the time being, as they should hopefully do that well enough themselves.

The domains to be searched should be listed in [domains.txt](domains.txt), which will be expected to be a plaintext file. Separate domain entries with a newline. By request, the script will prioritise finish searching all archives for one domain, letting the domain's history be completed. Only domains will be accepted, and will implicitly include all subdomains. Full URLs are not supported, because when combined with the implicit inclusion of subdomains, this would be less optimal.

### Configuration
Collector allows for an optional configuration file to overwrite default values. The format is very a very strict `key=value` combination. You should not use quotes for strings. The available configuration options are:

#### archive_host
**string**
Default: https://data.commoncrawl.org
The protocol and host of the archive server.

#### archive_list_uri
**string**
Default: /cc-index/collections/index.html
The request uri we want to send to get the list of archives.

#### max_file_size
**integer**
Default: 104857600
Largest file size allowed. Files above this size will be skipped. Only applies to archive files, as index files will generally be very large.

#### max_requests_limit, max_requests_time
**integer**
Default: 5
Two values controlling the frequency at which we will send requests. We will send no more than `max_requests_limit` requests every `max_requests_time seconds`.

#### cache_index_clusters
**boolean**
Default: False
Whether or not we should cache index clusters. Normally, this can be left off, since it's fairly unlikely we'll ever need to search the same cluster more than once.

#### pywb_collection_dir
**string**
Default: path/to/pywb/collection
This will need to be set to wherever your pywb collection is located.

#### safe_path
**string**
Default: `Path.cwd()`
A path that is considered safe, should point to where collector is located.

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