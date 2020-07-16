
# Index Command Line Interface

## Usage

The index executable will be located under the `bin` directory in the installation. 

For example, `<path to installtion directory>/bin/index` and must be executed from the `bin` directory because it uses relative paths by default.

```
Usage: index [-v] [--debug] [--disableLocking] --table=<table>
         [-c=<configDirPath>] [--column=<columns>[,<columns>...]]...
         [--partition=<partitions>[,<partitions>...]]...
         [--type=<indexTypes>[,<indexTypes>...]]... <command>

Using this index tool, you can CREATE, SHOW and DELETE indexes.

Supported index types: BITMAP, BLOOM, MINMAX

Supported data sources: HIVE using ORC files (must be configured in {--config}/catalog/catalog_name.properties


      <command>          command types, e.g. create, delete, show; Note: delete command
                           works a column level only.
      --column=<columns>[,<columns>...]
                         column, comma separated format for multiple columns
      --debug            if enabled the original data for each split will
                           also be written to a file alongside the index
      --disableLocking   by default locking is enabled at the table level; if this
                           is set to false, the user must ensure that the same data
                           is not indexed by multiple callers at the same time
                           (indexing different columns or partitions in parallel is
                           allowed)
      --partition=<partitions>[,<partitions>...]
                         only create index for these partitions, comma separated
                           format for multiple partitions
      --table=<table>    fully qualified table name
      --type=<indexTypes>[,<indexTypes>...]
                         index type, comma separated format for multiple types
                           (supported types: BLOOM, BITMAP, MINMAX
  -c, --config=<configDirPath>
                         root folder of openLooKeng etc directory (default: ../etc)
  -p, --plugins=<plugins>[,<plugins>...]
                         plugins dir or file, defaults to (default: .
                           /hetu-heuristic-index/plugins)
  -v                     verbose
```

## Examples

### Create index

``` shell
$ ./index -v -c ../etc --table hive.schema.table --column column1,column2 --type bloom,minmax,bitmap --partition p=part1 create
```

### Show index

``` shell
$ ./index -v -c ../etc --table hive.schema.table show
```

### Delete index

*Note:* index can only be deleted at table or column level, i.e. all index types will be deleted

``` shell
$ ./index -v -c ../etc --table hive.schema.table --column column1 delete
```

## Notes on resource usage

### Memory

By default the default JVM MaxHeapSize will be used (`java -XX:+PrintFlagsFinal -version | grep MaxHeapSize`). For improved performance, it is recommended to increase the MaxHeapSize. This can be
done by setting -Xmx value:

``` shell
export JAVA_TOOL_OPTIONS="-Xmx100G"
```

In this example the MaxHeapSize will be set to 100G.

### Indexing in parallel

If creating the index for a large table is too slow on one machine, you can create index for different partitions in parallel on different machines. This requires setting the --disableLocking flag and specifying the partition(s). For example:

On machine 1:

``` bash 
$ ./index -v ---disableLocking c ../etc --table hive.schema.table --columncolumn1,column2 --type bloom,minmax,bitmap --partition p=part1 create
```

On machine 2:

``` shell
$ ./index -v ---disableLocking c ../etc --table hive.schema.table --columncolumn1,column2 --type bloom,minmax,bitmap --partition p=part2 create
```
