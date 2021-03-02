
VACUUM
======

Synopsis
--------

``` sql
VACUUM TABLE table_name [FULL [UNIFY]] [PARTITION partition_value]? [AND WAIT]?
```

Description
-----------

BigData systems usually use HDFS as the storage to achieve durability and transparent distribution and balancing of data among the nodes in the cluster. HDFS being a immutable file system, data cannot be edited in between, but can only be appended. In order to work with the immutable file system, different file formats resort to writing new files in order to support data mutations, and later use asynchronous background merging to maintain the performance and avoid many small files.

For example, in Hive Connector you can update or delete ORC transactional table row by row. But whenever run update, an new delta and delete\_delta file will be generated in HDFS file system. Use `VACUUM` can merge all those small files to a larger file, and optimize parallelism and performance

**Types of VACUUMs:**

**Default**

Default vacuum can be treated as a first level of merging small data sets of the table. These will be frequent and usually will be faster compared to FULL vacuum.

*Hive:*

Default Vacuum corresponds to \'Minor Compaction' in Hive Connector. Merges all the valid delta directories into one compacted delta directory and similarly merge all valid delete\_delta directories to one
delete\_delta directory. The base file will not be changed. The old, smaller delta files will be removed once all readers are finished reading them.

**FULL**

FULL vacuum can be treated as the next level of merging of all data sets of table. These will be less frequent and takes longer time to complete compare to default vacuum.

**FULL UNIFY**

UNIFY option shall help to combine multiple bucket file of each partition into single bucket file with bucket number as zero. 

*Hive:*

FULL Vacuum corresponds to 'Major Compaction' in Hive Connector. Merges all base and delta files together. As part of this operation, the deleted or updated rows are permanently removed. All the aborted
transactions are removed from the transaction table in the metastore. The old delta files will be removed once all readers are finished reading them.

The `FULL` keyword indicate whether to start a Major Compaction. Without this option, it will do a Minor compaction;

Use `PARTITION` clause to specify which partition to vacuum.

Use `AND WAIT` to identify this vacuum running as synchronous mode. Without this option, it will run as asynchronous mode.

Examples
--------

Example 1: Default vacuum and wait for completion:

```sql
VACUUM TABLE compact_test_table AND WAIT;
```

Example 2: FULL vacuum on partition \'partition\_key=p1\':

```sql
VACUUM TABLE compact_test_table_with_partition FULL PARTITION 'partition_key=p1';
```

Example 3: FULL vacuum and wait for completion:

```sql
VACUUM TABLE compact_test_table_with_partition FULL AND WAIT;
```

Example 4: Unify all small files within 1 partition:

```sql
VACUUM TABLE catalog_sales FULL UNIFY PARTITION 'partition_key';
```



See Also
--------

[UPDATE](./update.md), [DELETE](./delete.md)
