# Common Crawl Collector
The purpose of this script is to search the [Common Crawl](https://commoncrawl.org) archives for specific domains, and download their matching content, for use with [pywb](https://pywb.readthedocs.io/). It has been written by Jens Nilsson Sahlin to aid research at Linköping University.

## Usage
The script is intended to run as a daemon over a long period of time. Other uses may be possible, but would not be advised without proper modifications.

Originally written to work alongside pywb, it also supports standalone downloading. Just reconfigure as needed. If used alongside pywb, it will depend on pywb running with automatic indexing enabled, and will employ a hack to ensure all files become indexed. Attempts were made to adress this issue using the wb-manager, but aside from having an extreme time cost, it would also sometimes fail to update the index properly. So generally problematic.

The domains to be searched should be listed in [domains.conf](domains.conf) (can be reconfigured by setting `domain_list_file`), which will be expected to be a plaintext file. Separate domain entries with a newline. By request, the script will prioritise finish searching all archives for one domain, letting the domain's history be completed. Only domains will be accepted, and will implicitly include all subdomains. Full URLs are not supported, because when combined with the implicit inclusion of subdomains, this would be less optimal.

### Configuration
Collector allows for an optional configuration file, `collector.conf`, to overwrite default values. The format is a very strict `key=value` combination. You should not use quotes for strings. The available configuration options are:

#### archive_host *(string)*
Default: https://data.commoncrawl.org

The protocol and host of the archive server.

#### archive_list_uri *(string)*
Default: /cc-index/collections/index.html

The request uri we want to send to get the list of archives.

#### max_file_size *(integer)*
Default: 104857600

Largest file size allowed. Files above this size will be skipped. Only applies to archive files, as index files will generally be very large.

#### min_request_interval *(float)*
Default: 1.0

Minimum time between sent HTTP requests.

#### max_request_interval *(float)*
Default: 30.0

Maximum time between sent HTTP requests, used for staggering failed HTTP requests.

#### cache_index_clusters *(boolean)*
Default: False

Whether or not we should cache index clusters. Normally, this can be left off, since it's fairly unlikely we'll ever need to search the same cluster more than once.

#### safe_path *(string)*
Default: `Path.cwd()`

A path that is considered safe, should point to where collector is located.

#### prometheus_port *(integer)*
Default: 1234

Which port to use for [Prometheus](https://prometheus.io) scraping.

#### cache_dir *(string)*
Default: .cache

Directory cache is stored in.

#### notification_email *(string)*
Default: None *(NoneType)*

Optional. Set to an email adress to have status updates (new archive/searches finished) sent to it. Also requires *mail_from_address* to be set.

#### mail_from_address *(string)*
Default: None *(NoneType)*

Optional. Required for functions related to sending email (outside of logging configuration file).

#### tempdir *(string)*
Default: /tmp/cccollector

Where to store temporary files. Needs to be on the same device as cache and collection dir.

#### indexing_method *(string)*
Default: auto

Indexing method:
 - 'none' (or 'download') means the collector will make no attempts to interact with pywb, making it able to function standalone. Requires `download_dir` to also be set.
 - 'auto' means the collector will make use of pywb's automatic indexing, and track files that remain unindexed to make use of a workaround. Will result in a slight increase in startup times. Requires `download_dir` to also be set.

#### download_dir *(string)*
Default: None *(NoneType)*

Where to drop files if automatic or no indexing method is used.

#### collection_name *(string)*
Default: root

#### pywb_collection_dir *(string)* (DEPRECATED)
Default: None

Use download_dir instead.

### remove_domain_archives.py
Usage: `./remove_domain_archive.py <domain>`

Removes all of: indexed archives for a specific domain, pywb index entries for the domain, and our own history file for the domain. The main purpose is to be used in testing, but may have other use-cases. **Recommended to not use while collector or pywb are running, and also, it relies on pywb having finished building the index.**

### status.py
`status.py` can be used to get completion information on individual domains. Usage is simple: `./status.py <all|domain>`. 

### find_unindexed_files.py and verify_history.py
Debugging scripts, should have little other use.

## Exceptions
The script comes with two custom exceptions. Both inherit from Exception with no additional changes. The exceptions are `ParserError` and `BadHTTPStatus`.

### ParserError
`ParserError` is the most important to take note of. It will be raised if we are unable to parse output of either the main index listing, or the cc-index.paths file, and should not be handled. This _could_ trigger if we get bad data from the server, but it will most likely mean there has been a format change, and our parsing algorithm will need to be updated accordingly.

### BadHTTPStatus
Will be raised if we get a non-OK HTTP (2??). Arguments passed, in order, are: url, offset, length, status code, and status message. Presently have handlers in relevant locations, and should not have any loose chains.

## License
Licensed under [GPLv3](https://www.gnu.org/licenses/gpl-3.0.html), see [license.txt](license.txt)