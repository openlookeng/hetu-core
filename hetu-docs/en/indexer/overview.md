
# openLooKeng Heuristic Indexer

## Introduction

Indexes can be created using one or more columns of a database table, providing faster random lookups. Most Big Data formats such as ORC, Parquet and CarbonData already have indices embedded in them.

The Heuristic Indexer allows creating indexes on existing data but stores the index external to the original data source. This provides several benefits:

  - The index is agnostic to the underlying data source and can be used by any query engine
  - Existing data can be indexed without having to rewrite the existing data files
  - New index types not supported by the underlying data source can be created
  - Index data does not use the storage space of the data source

## Use cases

Currently, heuristic indexer is supported on hive ORC data source to reduce the number of splits or rows read.

### 1. Filtering scheduled Splits during query execution

When the engine needs to schedule a TableScan operation, it schedules Splits on the workers. These Splits are responsible for reading a portion of the source data. However, not all Splits will return data if a predicate is applied.

By keeping an external index for the predicate column, the Heuristic Indexer can determine whether each split contains the values being searched for and only schedule the read operation for the splits which possibly contain the value.

![indexer_filter_splits](../images/indexer_filter_splits.png)

### 2. Filtering Block early when reading ORC files

When data needs to be read from an ORC file, the ORCRecordReader is used. This reader reads data from Stripes as batches (e.g. 1024 rows), which then form Pages. However, if a predicate is present, not all entries in the batch are required, some may be filtered out later by the Filter operator.

By keeping an external bitmap index for the predicate column, the Heuristic Indexer can filter out rows which do not match the predicates before the Filter operator is even applied.

## Example tutorial

This section gives a short tutorial which introduces the basic usage of heuristic index through a sample query.

### Identify a type of index on a column that can help

For a query:

    SELECT * FROM table1 WHERE id="abcd1234";
   
A bloom index on id column can significantly decrease the splits to read when scanning table1. 

We will use this example throughout this tutorial.

### Configure indexer settings

In `etc/config.properties`, add these lines:

Path white list：["/tmp", "/opt/hetu", "/opt/openlookeng", "/etc/hetu", "/etc/openlookeng", current workspace]

Notice：avoid to choose root directory; ../ can't include in path; if you config node.date_dir, then the current workspace is the parent of node.data_dir;
otherwise, the current workspace is the openlookeng server's directory.

    hetu.heuristicindex.filter.enabled=true
    hetu.heuristicindex.filter.cache.max-memory=2GB
    hetu.heuristicindex.indexstore.uri=/opt/hetu/indices
    hetu.heuristicindex.indexstore.filesystem.profile=index-store-profile
    
Then create an hdfs client profile in `etc/filesystem/index-store-profile.properties`, where `index-store-profile` is what specified above as the file name:

    fs.client.type=hdfs
    hdfs.config.resources=/path/to/core-site.xml,/path/to/hdfs-site.xml
    hdfs.authentication.type=NONE
    fs.hdfs.impl.disable.cache=true
    
If the hdfs cluster enables kerberos authentication, additional configs `hdfs.krb5.conf.path, hdfs.krb5.keytab.path, hdfs.krb5.principal` needs to be configured. 

In this example we use a hdfs cluster to store the index files, which can be shard across different hetu servers. If you would like to use local disk to store index, simply change `index-store-profile.properties` to:

    fs.client.type=local
    
Note that you may create multiple filesystem profiles in `etc/filesystem`, several for different hdfs clusters and one for local, so you can easily switch between them by just changing the value of `hetu.heuristicindex.indexstore.filesystem.profile`.

### Create index

To write index to the indexstore specified above, just change directory to your hetu installation's `bin` folder, then run:

    java -jar ./hetu-cli-*.jar --config <your-etc-folder-directory> --execute 'CREATE INDEX index_name USING bloom ON table1 (column)'
    
### Run query

After this is finished, run the query on hetu server again, and it will start loading index in the background. Keep running the same query while it's loading, then you should see decrease in the number of splits processed, which finally goes down to a rather small value.
