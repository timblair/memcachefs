# memcacheFS

memcacheFS is a proof-of-concept key-value store the uses the file system as the data store.  memcacheFS implements a sub-set of the memcached TCP text protocol to allow storage of arbitrarily sized data blobs.

## Available Commands

### `set`

### `get`

### `delete`

## Data Structure on Disk

If a key of `test_key` is given, of which the MD5 hash is `8c32d1183251df9828f929b935ae0419`, the data associated with that key will be stored on the filesystem in the following location (prepended with the directory path as given in the config):

	8c/
		32/
			8c32d1183251df9828f929b935ae0419
