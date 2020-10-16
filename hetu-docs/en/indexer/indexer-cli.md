

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

### Disk usage
Heuristic index uses local temporary storage to create the indices then tar them onto hdfs. Therefore, it requires enough local disk space where the temporary directory (e.g. `/tmp` for linux) is mounted. Users can specify the temporary directory for the index writer to use if the default one is mounted on the disk with insufficient space. To do this, run the cli with `-Djava.io.tmpdir` flag:

```bash
java -Djava.io.tmpdir=/path/to/another/dir -jar ./hetu-cli-*.jar
```

We have a rough estimation of how much disk space the indices may take for bloom index. It is linear to the original size of the table on which the index is created, and is linear to the negative logarithmic of `fpp`. The smaller fpp and larger dataset, the bigger index will be created:

index size = -log(fpp) * table size on disk * C

The coefficient C varies depending on some other factors, which are not as significant and usually change less, such as how much the selected column weighs in the table. As a typical value, its around 0.04. Therefore, for a typical table with a few columns which is 100GB on hdfs, creating a bloom index with `fpp=0.001` on a column will take about 12GB space, while it takes around 16GB if `fpp` is set at `0.0001`.

### Indexing in parallel

If creating the index for a large table is too slow on one machine, you can create an index for different partitions in parallel on different machines. Just make sure they are assigned with different partitions. For example:

On machine 1:

``` bash 
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute 'CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01") WHERE p=part1'
```

On machine 2:

``` shell
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute 'CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01") WHERE p=part2'
```
