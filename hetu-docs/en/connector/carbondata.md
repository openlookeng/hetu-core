


# Carbondata Connector

## Overview

The Carbondata connector allows querying data stored in a Carbondata warehouse. Carbondata is a combination of three components:

- Data files in carbondata storage formats that are typically stored in the Hadoop Distributed File System (HDFS).
- This metadata is only for table and column schema validation. carbondata metadata is stored along with the data files and is accessed via the Hive Metastore Service(HMS).
- A query language called HiveQL/SparkSQL. This query language is executed on a distributed computing framework such as MapReduce or Spark.

openLooKeng only uses the first two components: the data and the metadata. It does not use HiveQL/SparkSQL or any part of Hive’s execution environment.

**Note:** *Carbondata 2.0.1 is supported from openLooKeng*

## Configuration

The Carbondata connector supports Apache Hadoop 2.x and above.

Create `etc/catalog/carbondata.properties` with the following contents to mount the `carbondata` connector as the `carbondata` catalog, replacing `example.net:9083` with the correct host and port for your Hive Metastore Thrift service:

```properties
connector.name=carbondata
hive.metastore.uri=thrift://example.net:9083
```

### HDFS Configuration

For basic setups, openLooKeng configures the HDFS client automatically and does not require any configuration files. In some cases, such as when using federated HDFS or NameNode high availability, it is necessary to specify additional HDFS client options in order to access HDFS cluster. To do so, add the `hive.config.resources` property to reference your HDFS config files:

``` properties
hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml,/etc/hadoop/conf/yarn-site.xml,/etc/hadoop/conf/mapred-site.xml
```

Only specify additional configuration files if necessary for setup. It is also recommended reducing the configuration files to have the minimum set of required properties, as additional properties may cause problems.

The configuration files must exist on all openLooKeng nodes. If user is referencing existing Hadoop config files, make sure to copy them to any openLooKeng nodes that are not running Hadoop.

### HDFS Username and Permissions

Before running any `CREATE TABLE` or `CREATE TABLE AS` statements for Carbondata tables, openLooKeng should have access to Hive and HDFS. The Hive warehouse directory is specified by the configuration variable `hive.metastore.warehouse.dir` in `hive-site.xml`, and the default value is `/user/hive/warehouse`.

When not using Kerberos with HDFS, openLooKeng will access HDFS using the OS user of the openLooKeng process. For example, if openLooKeng is running as `nobody`, it will access HDFS as `nobody`. You can override this username by setting the `HADOOP_USER_NAME` system property in the openLooKeng [JVM Config](../installation/deployment.md#jvm-config), replacing `hdfs_user` with the appropriate username:

``` properties
-DHADOOP_USER_NAME=hdfs_user
```

The `hive` user generally works, since Hive is often started with the `hive` user and this user has access to the Hive warehouse.

Whenever you change the user which openLooKeng is using to access HDFS, remove `/tmp/openlookeng-*,/tmp/presto-*,/tmp/hetu-*` on HDFS, as new user may not have access to the existing temporary directories.

### Accessing Hadoop clusters protected with Kerberos authentication

Kerberos authentication is supported for both HDFS and the Hive metastore. However, Kerberos authentication by ticket cache is not yet supported.

The properties that apply to Carbondata connector security are listed in the [Carbondata Configuration Properties](./carbondata.md#carbondata-configuration-properties) table. Please see the [Hive Security Configuration](./hive-security.md) section for a more detailed discussion of the security options.

## Carbondata Configuration Properties

| Property Name                             | Description                                                  | Default                                         |
| ----------------------------------------- | :----------------------------------------------------------- | ----------------------------------------------- |
| `carbondata.store-location`                 | Specifies the location of the storage for carbondata warehouse. If not specified, it uses default hive warehouse path, i.e */user/hive/warehouse/**carbon.store*** | `${hive.metastore.warehouse.dir} /carbon.store` |
| `carbondata.minor-vacuum-seg-count`                 | Specifies the number of segments that may be considered for Minor Vacuum on a Carbondata table. If not specified or set to a number < 2, all available segments are considered | `NONE` |
| `carbondata.major-vacuum-seg-size`                 | Specifies the size limit (in GB) for Major Vacuum on a Carbondata table. All segments whose cumulative size is less than this threshold will be considered. If not specified, it uses default Vacuum value, i.e 1GB | `1GB` |
| `hive.metastore`                          | The type of Hive metastore to use. openLooKeng currently supports the default Hive Thrift metastore (`thrift`). | `thrift`                                        |
| `hive.config.resources`                   | A comma-separated list of HDFS configuration files. These files must exist on the machines running openLooKeng. Example: `/etc/hdfs-site.xml` |                                                 |
| `hive.hdfs.authentication.type`           | HDFS authentication type. Possible values are `NONE` or `KERBEROS`. | `NONE`                                          |
| `hive.hdfs.impersonation.enabled`         | Enable HDFS end user impersonation.                          | `false`                                         |
| `hive.hdfs.presto.principal`              | The Kerberos principal that openLooKeng will use when connecting to HDFS. |                                                 |
| `hive.hdfs.presto.keytab`                 | HDFS client keytab location.                                 |                                                 |
| `hive.collect-column-statistics-on-write` | Enables automatic column level statistics collection on write. See [Table Statistics](./hive.md#table-statistics) for details. | `true`                                          |
| `carbondata.vacuum-service-threads`       | Specifies number of threads for Auto-Vacuum & Auto-cleanup. Min value is 1. | 2                                               |
| `carbondata.auto-vacuum-enabled`          | Enable auto-vacuum on carbondata tables. To enable auto-vacuum on engine side, add `auto-vacuum.enabled=true` in config.properties of coordinator node(s). | false                                               |


## Hive Thrift Metastore Configuration Properties

| Property Name                        | Description                                                  |
| ------------------------------------ | ------------------------------------------------------------ |
| `hive.metastore.uri`                 | The URI(s) of the Hive metastore to connect to using the Thrift protocol. If multiple URIs are provided, the first URI is used by default and the rest of the URIs are fallback metastores. This property is required. Example: `thrift://192.0.2.3:9083` or `thrift://192.0.2.3:9083,thrift://192.0.2.4:9083` |
| `hive.metastore.username`            | The username openLooKeng will use to access the Hive metastore.     |
| `hive.metastore.authentication.type` | Hive metastore authentication type. Possible values are `NONE` or `KERBEROS` (defaults to `NONE`). |
| `hive.metastore.service.principal`   | The Kerberos principal of the Hive metastore service.        |
| `hive.metastore.client.principal`    | The Kerberos principal that openLooKeng will use when connecting to the Hive metastore service. |
| `hive.metastore.client.keytab`       | Hive metastore client keytab location.                       |

## Table Statistics

When writing data, the Carbondata connector always collects basic statistics (`numFiles`, `numRows`, `rawDataSize`, `totalSize`) and by default will also collect column level statistics:

| Column Type      | Null-Count | Distinct values count | Min/Max |
| ---------------- | ---------- | --------------------- | ------- |
| `SMALLINT `  | Y          | Y                     | Y       |
| `INTEGER`    | Y          | Y                     | Y       |
| `BIGINT`     | Y          | Y                     | Y       |
| `DOUBLE`    | Y          | Y                     | Y       |
| `REAL `     | Y          | Y                     | Y       |
| `DECIMAL `   | Y          | Y                     | Y       |
| `DATE `     | Y          | Y                     | Y       |
| `TIMESTAMP ` | Y          | Y                     | N       |
| `VARCHAR`    | Y          | Y                     | N       |
| `CHAR `     | Y          | Y                     | N       |
| `VARBINARY`  | Y          | N                     | N       |
| `BOOLEAN`    | Y          | Y                     | N       |

## Auto-cleanup

After the vacuum operation is completed on carbon tables, there will be unused base/stale folders & files (Ex: Segment folders, .segment and .lock  files) which are left in HDFS. So Carbondata Auto cleanup is used to cleanup those files automatically.
Segment folders and .segment files are deleted at the time of insert, update, delete and vacuum after 60min (.lock files after 48 hours) of vacuum operation is completed.    

## Examples

The Carbondata connector supports querying and manipulating Carbondata tables and schemas (databases). Most operations can be performed using openLooKeng, while some uncommon operations will need to be performed using Spark::Carbondata directly.

### Create Table

Create a new table `orders`:

```sql
CREATE TABLE orders (
    orderkey	bigint,
   	orderstatus varchar,
	totalprice 	double,
    orderdate	varchar
);
```

 

Supported properties

| Property | Description                                                  | Default  |
| -------- | ------------------------------------------------------------ | -------- |
| location | Specified directory is used to store table data. <br />If absent, default file system location will be used. | Optional |

Create a new table `orders` at specified location:

```sql
CREATE TABLE orders_with_store_location (
    orderkey 	bigint,
    orderstatus	varchar,
    totalprice	double,
    orderdate	varchar
 )
WITH ( location = '/store/path' );
```

**Note**:

- *If path is not fully qualified domain name, it will be stored in default file system.*



### Create Table as Select

Creates a new table based on the output of a SELECT statement

```sql
CREATE TABLE delivered_orders  
	AS SELECT * FROM orders WHERE orderstatus = 'Delivered';
```

```sql
CREATE TABLE backup_orders 
WITH ( location = '/backup/store/path' )
 	AS SELECT * FROM orders_with_store_location; 
```

### Insert

Load additional rows into the `orders` table:

```sql
INSERT INTO orders
VALUES (BIGINT '101', 'Ready', 1000.25, '2020-06-08');
```

Load additional rows into the `orders` table by overwriting the existing rows:

```sql
INSERT OVERWRITE orders 
VALUES (BIGINT '101', 'Delivered', 1040.25, '2020-06-26');
```

Load additional rows into the `orders` table with values from another table;

```sql
INSERT INTO orders 
SELECT * FROM delivered_orders;
```

### Update

Update all rows of table ``orders``:

```sql
UPDATE orders SET orderstatus = 'Ready';
```

Update  ``orders`` with filter condition:

```sql
UPDATE orders SET orderstatus = 'Delivered'
WHERE totalprice >1000 AND totalprice < 2000;
```

### Delete

Delete all rows in table ``orders``::

```sql
DELETE FROM orders;
```

Delete table ``orders`` with filter condition::

```sql
DELETE FROM orders WHERE orderstatus = 'Not Available';
```

### Drop Table

Drop an existing table.

```sql
DROP TABLE orders;
```

### Vacuum

Vacuum operation translates to `compaction` in Carbondata. In Carbondata there are two types of vacuum operation, `Major` and `Minor`.

The following is a short description of the mapping between VACUUM and Carbondata compaction-

* `VACUUM TABLE table_name FULL` translates to Major compaction which compacts segments based on a cumulative size restriction.

* `VACUUM TABLE table_name ` translates to Minor compaction which compacts segments based on a count restriction.

The below operation triggers Minor Vacuum on a table with name, `carbondata_table` to merge multiple segments to a single segment. 
```sql
VACUUM TABLE carbondata_table;
```

##### VACUUM FULL

Major Vacuum merges the segments based on their sizes. As long as the total size of all segments being merged doesn't exceed a certain value, they are all compacted into a single new segment.
```sql
VACUUM TABLE carbondata_table FULL;
```

##### Support for AND WAIT
Both Minor and Major Vacuum operations can be executed in a synchronous manner using the following commands-
```sql
VACUUM TABLE carbondata_table (FULL) AND WAIT;
```

This will also display the number of rows in the table that are being compacted.

## Carbondata Connector Limitations

The following operations are not supported currently with Carbondata connector:

- `sort_by`, `bucketed_by` and `partitioned_by` table properties are not supported while `CREATE TABLE`.
- Materialized views are not supported.
- Complex data types such as Arrays, Lists and Maps are not supported.
- Alter table usage is not supported.
- Create and Insert operation is not supported on partitioned tables.
