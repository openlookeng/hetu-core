Iceberg Connector
==============

## Overview

openLooKeng Iceberg is an open table format for huge analytic datasets. The Iceberg connector allows querying data stored in files written in Iceberg format.
Iceberg data files can be stored in either Parquet, ORC format, as determined by the format property in the table definition. The table format defaults to ORC.

## Requirements

To use Iceberg, you need:

- Network access from the openLooKeng coordinator and workers to the distributed object storage.
- Access to a Hive metastore service (HMS) .
- Network access from the openLooKeng coordinator to the HMS. Hive metastore access with the Thrift protocol defaults to using port 9083.

## Hive metastore catalog

The Hive metastore catalog is the default implementation. When using it, the Iceberg connector supports the same metastore configuration properties as the Hive connector. At a minimum, hive.metastore.uri must be configured.

```properties
connector.name=iceberg
hive.metastore.uri=thrift://localhost:9083
```

### General configuration

These configuration properties are independent of which catalog implementation is used.

Iceberg general configuration properties
With the following content creation`etc/catalog/iceberg.properties`，please repalce`localhost:9083` with the right ip and port：

| Attribute name                      | Attribute value         |necessary        |           Description                         |
|:------------------------------------|:----------------------- |:-------|:-------------------------------------|
| connector.name                      | iceberg                 |true       |            connector.name                  |
| hive.metastore.uri                  | thrift://localhost:9083 |true      |           Hive connector.uri               |
| iceberg.file-format                 | ORC                     |false      | Define the data storage file format for Iceberg tables. Possible values are PARQUET、ORC |
|iceberg.compression-codec|ZSTD|false|The compression codec to be used when writing files. Possible values are (NONE SNAPPY LZ4 ZSTD GZIP)
|iceberg.use-file-size-from-metadata|true|false|Read file sizes from metadata instead of file system. This property should only be set as a workaround for this issue. The problem was fixed in Iceberg version 0.11.0.
|iceberg.max-partitions-per-writer|100|false|Maximum number of partitions handled per writer.
|iceberg.unique-table-location|true|false|Use randomized, unique table locations.
|iceberg.dynamic-filtering.wait-timeout|0s|false|Maximum duration to wait for completion of dynamic filters during split generation.
|iceberg.table-statistics-enabled|true|false|Enables Table statistics. The equivalent catalog session property is for session specific use. Set to to disable statistics. Disabling statistics means that Cost based optimizations can not make smart decisions about the query plan.statistics_enabledfalse
|iceberg.minimum-assigned-split-weight|0.05|false|A decimal value in the range (0, 1] used as a minimum for weights assigned to each split. A low value may improve performance on tables with small files. A higher value may improve performance for queries with highly skewed aggregations or joins.
## SQL support

This connector provides read access and write access to data and metadata in Iceberg. In addition to the globally available and read operation statements, the connector supports the following features:

### Table Related Statements
- create
```sql
CREATE TABLE ordersg (
    order_id BIGINT,
    order_date DATE,
    account_number BIGINT,
    customer VARCHAR,
    country VARCHAR)
WITH (partitioning = ARRAY['month(order_date)', 'bucket(account_number, 10)', 'country']);
```
### Column related statements
- INSERT
```sql
alter table ordersg add COLUMN zip varchar;
```
- RENAME
```sql
ALTER TABLE ordersg RENAME COLUMN zip TO zip_r;
```
- DELETE
```sql
ALTER TABLE ordersg DROP COLUMN zip_r;
```
### Data related Statements
- INSERT
```sql
insert into ordersg values(1,date'1988-11-09',666888,'tim','US');
```
- DELETE
```sql
Delete from ordersg where order_id = 1;
```
- UPDATE
```sql
update ordersg set customer = 'Alice' where order_id = 2;
```
- SELECT
```sql
select * from ordersg; 
```

## Partitioned tables

Iceberg supports partitioning by specifying transforms over the table columns. A partition is created for each unique tuple value produced by the transforms. Identity transforms are simply the column name. Other transforms are:

|          Transform             | Description                                                                                                                                              |
|:------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------|
| year(ts) | A partition is created for each year. The partition value is the integer difference in years between ts and January 1 1970.                              |
| month(ts) | A partition is created for each month of each year. The partition value is the integer difference in months between ts and January 1 1970.               |
| day(ts)  | A partition is created for each day of each year. The partition value is the integer difference in days between ts and January 1 1970.                   |
| hour(ts) | A partition is created hour of each day. The partition value is a timestamp with the minutes and seconds set to zero.                                    |
| bucket(x,nbuckets)     | The data is hashed into the specified number of buckets. The partition value is an integer hash of x, with a value between 0 and nbuckets - 1 inclusive. |
| truncate(s,nchars)     | The partition value is the first nchars characters of s.                                                                                                 |

In this example, the table is partitioned by the month of order_date, a hash of account_number (with 10 buckets), and country:
```sql
CREATE TABLE ordersg (
                         order_id BIGINT,
                         order_date DATE,
                         account_number BIGINT,
                         customer VARCHAR,
                         country VARCHAR)
    WITH (partitioning = ARRAY['month(order_date)', 'bucket(account_number, 10)', 'country']);
```
Manually Modifying Partitions
```sql
ALTER TABLE ordersg SET PROPERTIES partitioning = ARRAY['month(order_date)'];
```
For partitioned tables, the Iceberg connector supports the deletion of entire partitions if the WHERE clause specifies filters only on the identity-transformed partitioning columns, that can match entire partitions. Given the table definition above, this SQL will delete all partitions for which country is US:

```sql
DELETE FROM iceberg.testdb.ordersg WHERE country = 'US';
```

### Rolling back to a previous snapshot

Iceberg supports a “snapshot” model of data, where table snapshots are identified by an snapshot IDs.

The connector provides a system snapshots table for each Iceberg table. Snapshots are identified by BIGINT snapshot IDs. You can find the latest snapshot ID for table customer_orders by running the following command:
```sql
SELECT snapshot_id FROM "ordersg $snapshots" ORDER BY committed_at DESC LIMIT 1;
```

|snapshot_id|
|:----------|
|921254093881523606|
|535467754709887442|
|343895437069940394|
|34i302849038590348|
|(4 rows)|

A SQL procedure system.rollback_to_snapshot allows the caller to roll back the state of the table to
a previous snapshot id:
```sql
CALL iceberg.system.rollback_to_snapshot('testdb', 'ordersg', 8954597067493422955);
```

## Metadata tables

The connector exposes several metadata tables for each Iceberg table. These metadata tables contain information about the internal structure of the Iceberg table. You can query each metadata table by appending the metadata table name to the table name:

```sql
SELECT * FROM "ordersg$data";
```

### $data table#

The $data table is an alias for the Iceberg table itself.

The statement:
```sql
SELECT * FROM "ordersg$data";
```
is equivalent to:
```sql
SELECT * FROM ordersg;
```

### $properties table
The $properties table provides access to general information about Iceberg table configuration and any additional metadata key/value pairs that the table is tagged with.

You can retrieve the properties of the current snapshot of the Iceberg table  by using the following query:
```sql
SELECT * FROM "ordersg$properties";
```

key                   | value    |
-----------------------+----------+
write.format.default   | PARQUET  |


### $history table#

The $history table provides a log of the metadata changes performed on the Iceberg table.

You can retrieve the changelog of the Iceberg table by using the following query:
```sql
SELECT * FROM "ordersg$history";
```

made_current_at                  | snapshot_id          | parent_id            | is_current_ancestor
----------------------------------+----------------------+----------------------+--------------------
2022-08-19 05:42:37.854 UTC  |   7464177163099901858  | 7924870943332311497   |  true
2022-08-19 05:44:35.212 UTC  |   2228056193383591891  | 7464177163099901858  |  true

The output of the query has the following columns:

| Name	               | Type                        |Description	|
|:--------------------|:----------------------------|:----------------------|
| made_current_at     | 	timestamp(3)with time zone |The time when the snapshot became active	|
| snapshot_id         | 	bigint                     |The identifier of the snapshot	|
| parent_id           | 	bigint	                   |The identifier of the parent snapshot|
| is_current_ancestor | 	boolean	                   |Whether or not this snapshot is an ancestor of the current snapshot|

### $snapshots table

The $snapshots table provides a detailed view of snapshots of the Iceberg table. A snapshot consists of one or more file manifests, and the complete table contents is represented by the union of all the data files in those manifests.

You can retrieve the information about the snapshots of the Iceberg table  by using the following query:

```sql
SELECT * FROM "ordersg$snapshots";
```

|   committed_at   |   snapshot_id   |   parent_id   |   operation   |   manifest_list   |   summary   |
|   2022-08-08 08:20:04.126 UTC   |   7026585913702073835   |              |   append   |   hdfs://hadoop1:9000/home/gitama/hadoop/hive/user/hive/warehouse/test_100.db/orders08/metadata/snap-7026585913702073835-1-d0b5ba3d-6363-4f32-974e-79bb68d19423.avro   |   {changed-partition-count=0, total-equality-deletes=0, total-position-deletes=0, total-delete-files=0, total-files-size=0, total-records=0, total-data-files=0}   |
|   2022-08-08 08:21:58.343 UTC   |   629134202395791160   |   7026585913702073835   |   append   |   hdfs://hadoop1:9000/home/gitama/hadoop/hive/user/hive/warehouse/test_100.db/orders08/metadata/snap-629134202395791160-1-b6e9c1c3-0532-4bf8-a814-a159494e272d.avro   |   {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=1, total-position-deletes=0, added-files-size=289, total-delete-files=0, total-files-size=289, total-records=1, total-data-files=1}   |

The output of the query has the following columns:

| Name          |                  Type          | 	        Description                                                                                                                                                                                                                                                                                                                                        |
|:--------------|:-----------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| committed_at	 | timestamp(3) with time zone	 | The time when the snapshot became active                                                                                                                                                                                                                                                                                                                    |
| snapshot_id   | 	bigint	                     | The identifier for the snapshot                                                                                                                                                                                                                                                                                                                             |
| parent_id	    |bigint	| The identifier for the parent snapshot                                                                                                                                                                                                                                                                                                                      | 
| operation	    | varchar                      | The type of operation performed on the Iceberg table. The supported operation types in Iceberg are: -append when new data is appended -replace when files are removed and replaced without changing the data in the table -overwrite when new data is added to overwrite existing data  -delete when data is deleted from the table and no new data is added|
| manifest_list | 	varchar	                    | The list of avro manifest files containing the detailed information about the snapshot changes.                                                                                                                                                                                                                                                             |
| summary	      | map(varchar, varchar)	       | A summary of the changes made from the previous snapshot to the current snapshot                                                                                                                                                                                                                                                                            |

### $manifests table#

The $manifests table provides a detailed overview of the manifests corresponding to the snapshots performed in the log of the Iceberg table.
You can retrieve the information about the manifests of the Iceberg table  by using the following query:
```sql
SELECT * FROM "ordersg$manifests";
```

Path   |   length   |   partition_spec_id   |   added_snapshot_id  |  added_data_files_count   |   existing_data_files_count   |   deleted_data_files_count     |   partitions              
---------------------------------------+---------------------+---------------------+---------------------+-------------------+--------------------+--------------------+--------------------+--------------------+-------------------
hdfs://hadoop1:9000/home/gitama/hadoop/hive/user/hive/warehouse/test_100.db/orders08/metadata/b6e9c1c3-0532-4bf8-a814-a159494e272d-m0.avro  |  6534  |  0  | 629134202395791160 | 1 | 0 | 0 | [ ]

The output of the query has the following columns:

|Name| 		Type           | Description                                                        |
|:----|:---------------------|:-------------------------------------------------------------------|
|path| 	varchar	         | The manifest file location                                         |
|length	| bigint	        | length                                                             |
|partition_spec_id| 	integer             | partition_spec_id	                                                 |
|added_snapshot_id	| bigint          | 	added_snapshot_id                                                 |
|added_data_files_count| 	integer	      | The number of data files with status ADDED in the manifest file    |
|existing_data_files_count	| integer	   | The number of data files with status EXISTING in the manifest file |
|deleted_data_files_count	| integer	  | The number of data files with status DELETED in the manifest file  |
|partitions| 	array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar)) | 	Partition range metadata                                          |


### $partitions table

The $partitions table provides a detailed overview of the partitions of the Iceberg table.
You can retrieve the information about the partitions of the Iceberg table  by using the following query:
```sql
SELECT * FROM "ordersg$partitions";
```

|   record_count   |   file_count   |   total_size   |   data   |   --------------+-------------+---------------------+---------------------+------------------------------------|   1   |   1   |   289   |   {id={min=null, max=null, null_count=0, nan_count=null}, name={min=null, max=null, null_count=0, nan_count=null}}   |

The output of the query has the following columns:
Partitions columns

|Name|Type	|Description	|
|:-----|:-----|:-------|
|record_count	|bigint	|The number of records in the partition|
|file_count|	bigint|The number of files mapped in the partition	|
|total_size	|bigint|The size of all the files in the partition	|
|data	|row(... row (min ..., max ... , null_count bigint, nan_count bigint))	|Partition range metadat|

### $files table#

The $files table provides a detailed overview of the data files in current snapshot of the Iceberg table.

To retrieve the information about the data files of the Iceberg table use the following query:
```sql
SELECT * FROM "ordersg$files";
```

| content   |   file_path   |   file_format   |   record_count   |   file_size_in_bytes   |   column_sizes   |   value_counts   |   null_value_counts   |   nan_value_counts   |   lower_bounds   |   upper_bounds   |   key_metadata   |   split_offsets   |   equality_ids   |   
|  0   |   hdfs://192.168.31.120:9000/user/hive/warehouse/orders19/data/20220819_034313_39152_vdmku-1709db2a-dc6f-4ef9-bb77-23f4c150801f.orc   |   ORC   |   1   |   354   |      |   {1=1, 2=1, 3=1}","{1=0, 2=0, 3=0}   |      |      |      |      |      |      |   
|   0   |   hdfs://192.168.31.120:9000/user/hive/warehouse/orders19/data/20220819_054009_11365_xq568-1803130c-6b7b-4da6-b460-dfb44f176ef4.orc   |   ORC   |   1   |   413   |      |   {1=1, 2=1, 3=1, 4=1}   |   {1=0, 2=0, 3=0, 4=1}   |      |      |      |      |      |      |

The output of the query has the following columns:
Files columns

|Name|	Type	| Description                                                                                           |
|:----|:--------|:------------------------------------------------------------------------------------------------------|
|content|	integer| 	Type of content stored in the file. The supported content types in Iceberg are: -DATA(0) - POSITION_DELETES(1) - EQUALITY_DELETES(2)|
|file_path|	varchar| 	The data file location                                                                               |
|file_format|	varchar	| The format of the data file                                                                           |
|record_count|	bigint	| The number of entries contained in the data file                                                      |
|file_size_in_bytes	|bigint	| The data file size                                                                                    |
|column_sizes|	map(integer, bigint)	| Mapping between the Iceberg column ID and its corresponding size in the file                          |
|value_counts|	map(integer, bigint)	| Mapping between the Iceberg column ID and its corresponding count of entries in the file              |
|null_value_counts|	map(integer, bigint)	| Mapping between the Iceberg column ID and its corresponding count of NULL values in the file          |
|nan_value_counts|	map(integer, bigint)	| Mapping between the Iceberg column ID and its corresponding count of non numerical values in the file |
|lower_bounds|	map(integer, bigint)	| Mapping between the Iceberg column ID and its corresponding lower bound in the file                   |
|upper_bounds|	map(integer, bigint)	| Mapping between the Iceberg column ID and its corresponding upper bound in the file                   |
|key_metadata	|varbinary	| Metadata about the encryption key used to encrypt this file, if applicable                            |
|split_offsets|	array(bigint)	| List of recommended split locations                                                                   |
|equality_ids	|array(integer)	| The set of field IDs used for equality comparison in equality delete files                            |



### ALTER TABLE EXECUTE

The connector supports the following commands for use with ALTER TABLE EXECUTE(For details, see Merging Files).

### File merging

The optimize command is used for rewriting the active content of the specified table so that it is merged into fewer but larger files. In case that the table is partitioned, the data compaction acts separately on each partition selected for optimization. This operation improves read performance.

All files with a size below the optional file_size_threshold parameter (default value for the threshold is 100MB) are merged:
```sql
ALTER TABLE ordersg EXECUTE optimize;
```
The following statement merges the files in a table that are under 10 megabytes in size:
```sql
ALTER TABLE ordersg EXECUTE optimize(file_size_threshold => '10MB');
```

### ALTER TABLE SET PROPERTIES

The connector supports modifying the properties on existing tables using ALTER TABLE SET PROPERTIES.

The following table properties can be updated after a table is created:
- format
- partitioning

```sql
ALTER TABLE ordersg SET PROPERTIES format ='PARQUET';
```

Or to set the column country as a partition column on a table:
```sql
ALTER TABLE ordersg SET PROPERTIES partitioning = ARRAY[<existing partition columns>, 'country'];
```

You can use SHOW CREATE TABLE Ordersg to display the current values of the TABLE properties.


### openLooKeng to Iceberg type mapping
| openLooKeng type                   | Iceberg type
|:-----------------------|:------------------------
|BOOLEAN               | BOOLEAN
|INTEGER               | INT
|BIGINT               | LONG
|REAL               | FLOAT
|DOUBLE               | DOUBLE
|DECIMAL(p,s)               | DECIMAL(p,s)
|DATE               | DATE
|TIME               | TIME
|TIMESTAMP               | TIMESTAMP
|TIMESTAMP WITH TIME ZONE               | TIMESTAMPTZ
|VARCHAR               | STRING
|VARBINARY               | BINARY
|ROW(...)            |STRUCT(...)
|ARRAY(e)            |LIST(e)
|MAP(k,v)           |MAP(k,v)







