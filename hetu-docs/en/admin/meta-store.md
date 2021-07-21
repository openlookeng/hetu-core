# Meta Store
This section describes the openLooKeng meta store. Meta store is used to store metadata. Meta store supports to 
store metadata in the RDBMS or HDFS.

## Configuring Meta Store

Create the meta store property file `etc/hetu-metastore.properties` first.


### RDBMS Storage

Add the following contents in `hetu-metastore.properties`:

``` properties
hetu.metastore.type=jdbc
hetu.metastore.db.url=jdbc:mysql://....
hetu.metastore.db.user=root
hetu.metastore.db.password=123456
hetu.metastore.cache.type=local
```

The above properties are described below:

- `hetu.metastore.type`：The type of meta store, set `jdbc` to use RDBMS storage.
- `hetu.metastore.db.url`：URL of RDBMS to connect to.
- `hetu.metastore.db.user` :User name of RDBMS to connect to. 
- `hetu.metastore.db.password` :Password of RDBMS to connect to.
- `hetu.metastore.cache.type` : Select the cache model, where local is the local cache and global is the distributed cache.

### HDFS Storage

Add the following contents in `hetu-metastore.properties`:

```
hetu.metastore.type=hetufilesystem
hetu.metastore.hetufilesystem.profile-name=hdfs-config-metastore
hetu.metastore.hetufilesystem.path=/etc/openlookeng/metastore
```

The above properties are described below:

- `hetu.metastore.type`：The type of meta store, set `hetufilesystem` to use HDFS storage.
- `hetu.metastore.hetufilesystem.profile-name`：The profile name of file system.
- `hetu.metastore.hetufilesystem.path`：The path of metastore storage in the file system.

### Meta Store Cache

openLooKeng can be configured to enable metadata caching. After the metadata caching is enabled, metadata will be cached 
in the memory to improve access efficiency for the next access.

Add the following contents in `hetu-metastore.properties`:

``` properties
hetu.metastore.cache.size=10000
hetu.metastore.cache.ttl=4h     
```

The above properties are described below:

- `hetu.metastore.cache.size`：Set the max metastore cache size, default value 10000.
- `hetu.metastore.cache.ttl`：Set ttl for metastore cache, default value 0 (metadata caching is disabled).
