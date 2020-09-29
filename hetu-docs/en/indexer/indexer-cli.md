
# Index Command Line Interface

## Usage

The indexer can be utilized using the hetu-cli executable located under the `bin` directory in the installation. 

To create an index you can run sql queries of the form:
```roomsql  
CREATE INDEX [ IF NOT EXISTS ] index_name
USING [ BITMAP | BLOOM | MINMAX ]
ON tbl_name (col_name)
WITH ( "bloom.fpp" = '0.001', [, …] )
WHERE predicate;
```

To show all indexes or a specific index_name: 
```roomsql
SHOW INDEX;
SHOW INDEX index_name;
```

To delete an index by name:
```roomsql
DROP INDEX index_name;
```

## Examples

Path white list：["/tmp", "/opt/hetu", "/opt/openlookeng", "/etc/hetu", "/etc/openlookeng", current workspace]

`etc` directory includes config.properties, and --config should specify an absolute path,
the path should be children directory of Path white list

### Create index

``` shell
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute 'CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01", verbose=true) WHERE p=part1'
```

### Show index

``` shell
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute "SHOW INDEX index_name"
```

### Drop index

``` shell
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute "DROP INDEX index_name"
```

*Note*: Dropping an index will not remove the cached index from hetu server. This means the index may still be used until it expires from cache based on `hetu.heuristicindex.filter.cache.ttl` value or hetu server is restarted.

## Notes on resource usage

### Memory

By default the default JVM MaxHeapSize will be used (`java -XX:+PrintFlagsFinal -version | grep MaxHeapSize`). For improved performance, it is recommended to increase the MaxHeapSize. This can be
done by setting -Xmx value:

*Note*: This should be done before executing the hetu-cli.
``` shell
export JAVA_TOOL_OPTIONS="-Xmx100G"
```

In this example the MaxHeapSize will be set to 100G.

### Indexing in parallel

If creating the index for a large table is too slow on one machine, you can create an index for different partitions in parallel on different machines. This requires setting the parallelCreation property to true and specifying the partition(s). For example:

On machine 1:

``` bash 
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute 'CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01", parallelCreation=true) WHERE p=part1'
```

On machine 2:

``` shell
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute 'CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01", parallelCreation=true) WHERE p=part2'
```
