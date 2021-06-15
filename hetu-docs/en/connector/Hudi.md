# Hudi  Connector

### Hudi Introduction

Apache Hudi is a fast growing data lake storage system that helps organizations build and manage petabyte-scale data lakes. Hudi enables storing vast amounts of data on top of existing DFS compatible storage while also enabling stream processing in addition to typical batch-processing. This is made possible by providing two new primitives. Specifically,

- **Update/Delete Records**: Hudi provides support for updating/deleting records, using fine grained file/record level indexes, while providing transactional guarantees for the write operation. Queries process the last such committed snapshot, to produce results.
- **Change Streams**: Hudi also provides first-class support for obtaining an incremental stream of all the records that were updated/inserted/deleted in a given table, from a given point-in-time, and unlocks a new incremental-query category.

### Supported Table types and Query types

**Hudi supports the following table types**

**Copy On Write (COW)**: Stores data using exclusively columnar file formats (e.g parquet). Updates version & rewrites the files by performing a synchronous merge during write.

**Merge On Read (MOR)**: Stores data using file versions with combination of columnar (e.g parquet) + row based (e.g avro) file formats. Updates are logged to delta files & later compacted to produce new versions of columnar files synchronously or asynchronously.

**Hudi supports the following query types**

**Snapshot Queries**: Queries see the latest snapshot of the table as of a given commit or compaction action. In case of merge-on-read table, it exposes near-real time data (few mins) by merging the base and delta files of the latest file version on-the-fly. For copy-on-write tables, it provides a drop-in replacement for existing parquet tables, while providing upsert/delete and other write side features.

**Read Optimized Queries**: Queries see the latest snapshot of a table as of a given commit/compaction action. Exposes only the base/columnar files in latest file versions and guarantees the same columnar query performance compared to a non-hudi columnar table.

## Configuration

Typically, we ingest the data from external data sources(e.g. Kafka) and write it to a DFS file storage system, such as HDFS. Then we synchronize Hudi data from HDFS to the Hive table through the data synchronization tool provided by Hudi. The Hudi tables are stored as the external tables in Hive. After that, we query the Hudi table by connecting to the Hive Connector. For the generation of the Hudi table and the steps to synchronize to the Hive table, please refer to the official [Hudi demo example](https://hudi.apache.org/docs/docker_demo.html).

First, before you start using the Hudi connector, you should complete the following steps:

- JDBC connection details for connecting to the Oracle database

Configure Hudi Connector (i.e. Hive Connector). Create `etc/catalog/hive.properties` with the following contents to mount the `hive-hadoop2` connector as the `hive` catalog, replacing `example.net:9083` with the correct host and port for your Hive metastore Thrift service:

``` properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://example.net:9083
```

- Adding the Hudi driver （Hudi version is 0.7.0）

```properties
# Download Hudi source code and compile
cd /opt
wget https://github.com/apache/hudi/releases/tag/release-0.7.0
tar -zxvf hudi-release-0.7.0.tar.gz
cd hudi-release-0.7.0
mvn package -DskipTests
```

After compilation, enter the folder `/opt/hudi-release-0.7.0/packaging/hudi-presto-bundle/target/`, move the `hudi-presto-bundle-0.7.0.jar` package into the openLooKeng plugin package folder. E.g. if the openLooKeng plugin package folder is **/opt/hetu-server-1.3.0-SNAPSHOT/plugin**, the use the following command: **cp /opt/hudi-presto-bundle-0.7.0.jar /opt/hetu-server-1.3.0-SNAPSHOT/plugin/hive-hadoop2**. At this point, to prevent dependency jar conflicts while reading the Hudi table, we should delete `hudi-common-0.8.0.jar` and `hudi-hadoop-mr-0.8.0.jar`. Meanwhile, we should copy the extra package `jackson-mapper-asl-1.9.13.jar` and `jackson-core-asl-1.9.13.jar` into the current hive-haddop2 plugin folder (After Hudi compilation, these two jars could be found in location /opt/hudi-release-0.7.0/hudi-cli/target/lib). 

```properties
# copy Hudi driver package
cp hudi-presto-bundle-0.7.0.jar /opt/hetu-server-1.3.0-SNAPSHOT/plugin/hive-hadoop2
# delete `hudi-common-0.8.0.jar`, `hudi-hadoop-mr-0.8.0.jar` package
cd /opt/hetu-server-1.3.0-SNAPSHOT/plugin/hive-hadoop2
rm -rf hudi-common-0.8.0.jar hudi-hadoop-mr-0.8.0.jar
# compile Hudi source code and find `jackson-mapper-asl-1.9.13.jar` and `jackson-core-asl-1.9.13.jar`，put them into the current folder
cp /opt/hudi-release-0.7.0/hudi-cli/target/lib/jackson-mapper-asl-1.9.13.jar .
cp /opt/hudi-release-0.7.0/hudi-cli/target/lib/jackson-core-asl-1.9.13.jar .
```

## Query Hudi COW Table

Refer to [Hudi demo example](https://hudi.apache.org/docs/docker_demo.html), we could generate Hudi COW table（**stock_ticks_cow**), then we could query the Hudi table using the following statement:

    # COPY-ON-WRITE Queries: 
    select "_hoodie_commit_time", symbol, ts, volume, open, close from stock_ticks_cow where symbol = 'GOOG';

Current Query results（**Snapshot Queries**）：

    lk:default> select "_hoodie_commit_time", symbol, ts, volume, open, close from stock_ticks_cow where symbol = 'GOOG';
    _hoodie_commit_time | symbol |        ts          | volume | open    | close
    --------------------+--------+--------------------+--------+---------+---------
      20210519083634    | GOOG   | 2018-08-31 09:59:00|  6330  |1230.5   | 1230.02
      20210519083834    | GOOG   | 2018-08-31 10:59:00|  3391  |1227.1993| 1227.215
    (2 rows)

As you can see from the current snapshot query, the latest ts time is 2018-08-31 10:59:00, which is already the most recently updated data

## Query Hudi MOR Table

Refer to [Hudi demo example](https://hudi.apache.org/docs/docker_demo.html), we could generate Hudi COW table（**stock_ticks_mor_ro**), then we could query the Hudi table using the following statement:

    # Merge-On-Read Queries:
    select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
    

Current Query results（**Read Optimized Queries**）：

    lk:default> select "_hoodie_commit_time", symbol, ts, volume, open, close from stock_ticks_mor_ro where symbol = 'GOOG';
    _hoodie_commit_time | symbol |        ts          | volume | open    | close
    --------------------+--------+--------------------+--------+---------+---------
      20210519083634    | GOOG   | 2018-08-31 09:59:00|  6330  |1230.5   | 1230.02
      20210519083634    | GOOG   | 2018-08-31 10:29:00|  3391  |1230.1899| 1230.085
    (2 rows)

As you can see from the current read optimization query, the latest ts time is 2018-08-31 10:29:00, and we obtain this result by just reading base/columnar files in latest file versions

Refer to [Hudi demo example](https://hudi.apache.org/docs/docker_demo.html), we could generate Hudi COW table（**stock_ticks_mor_rt**), then we could query the Hudi table using the following statement:

    # Merge-On-Read Queries:
    select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';

Current Query results（**Snapshot Queries**）：

    lk:default> select "_hoodie_commit_time", symbol, ts, volume, open, close from stock_ticks_mor_rt where symbol = 'GOOG';
    _hoodie_commit_time | symbol |        ts          | volume | open    | close
    --------------------+--------+--------------------+--------+---------+---------
      20210519083634    | GOOG   | 2018-08-31 09:59:00|  6330  |1230.5   | 1230.02
      20210519083834    | GOOG   | 2018-08-31 10:59:00|  3391  |1227.1993| 1227.215
    (2 rows)

As you can see from the current snapshot query, the latest ts time is 2018-08-31 10:59:00, which is already the most recently updated data, indicating that we have merged the base and delta files of the latest file version on-the-fly

## Hudi Connector Limitations

- Only snapshot queries for Hudi COW table is supported
- Only snapshot queries and read optimization queries for Hudi MOR table are supported

