# HBase Connector

------

## Overview

The HBase Connector allows querying and creating tables on an external Apache HBase instance. Users can create a table in HBase connector, mapping it to an existing table in HBase Cluster, and support insert, select, or delete.

The HBase Connector maintains a Metastore to persist HBase metadata, currently support Metastore: `openLooKeng Metastore`.

**Note:** *Apache HBase 2.2.3 version or ealier versions are supported in HBase connector*



## Connector Configuration

1 . Create and edit `etc/catalog/hbase.properties` and replacing the `xxx` as required; connector.name is fixed. If the value of `hbase.zookeeper.quorum` contains multiple IP addresses, use commas (,) as separators.

```properties
connector.name=hbase-connector
hbase.zookeeper.quorum=xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx
hbase.zookeeper.property.clientPort=xxxx
```

2 . Use openlookeng metadata storage to store hbase metadata

You must create `etc/hetu-metastore.properties` to configure the metadata storage platform. The storage platform can be RDBMS or HDFS. Configuration example:

vi etc/hetu-metastore.properties

Option 1: Set hetu-metastore to RDBMS.
```
hetu.metastore.type=jdbc
hetu.metastore.db.url=jdbc:mysql://xx.xx.xx.xx:3306/dbName....
hetu.metastore.db.user=root
hetu.metastore.db.password=123456
```

Option 2：Set hetu-metastore to HDFS：
```properties
hetu.metastore.type=hetufilesystem
# profile name of hetu file system
hetu.metastore.hetufilesystem.profile-name=hdfs-config-metastore
#the path of metastore storage in the hetu file system
hetu.metastore.hetufilesystem.path=/etc/openlookeng/metastore
```    
If HDFS is used, configure hetu filesystem. The configuration file name must be the same as that of profile-name, and the suffix is properties.

vi etc/filesystem/hdfs-config-metastore.properties
```properties
fs.client.type=hdfs
hdfs.config.resources=/opt/openlookeng/xxx/core-site.xml,/opt/openlookeng/xxx/hdfs-site.xml
hdfs.authentication.type=NONE
```  
For details about HDFS Kerberos configurations, see [filesystem](../develop/filesystem.md).

**Note:** *Either the hbase.properties and hetu-metastore.properties configuration files are mandatory. If HDFS is configured, hdfs-config-metastore.properties is required.*

```
Possible problem: After the configuration is complete, an error message indicating that the xxx path (configuration file A) is not in the white path list is displayed when the service is started.
Solution: Select a directory in the white path list recorded in the log, copy the file in the directory where the error occurs, and modify the corresponding path in configuration file A.
```

**Kerberos Configuration:**

If HBase/Zookeeper is a security cluster，so we should add the configuration about kerberos.

```properties
hbase.jaas.conf.path=/opt/openlookeng/xxx/jaas.conf

hbase.hbase.site.path=opt/openlookeng/xxx/hbase-site.xml

hbase.krb5.conf.path=opt/openlookeng/xxx/krb5.conf

hbase.kerberos.keytab=opt/openlookeng/xxx/user.keytab

hbase.kerberos.principal=lk_username@HADOOP.COM

hbase.authentication.type=KERBEROS
```

vi jaas.conf
```properties
Client {
com.sun.security.auth.module.Krb5LoginModule required
useKeyTab=true
keyTab="opt/openlookeng/xxx/user.keytab"
principal="lk_username@HADOOP.COM"
useTicketCache=false
storeKey=true
debug=true;
};
```
## Configuration Properties

| Property Name                       | Default Value | Required | Description                                                  |
| ----------------------------------- | ------------- | -------- | ------------------------------------------------------------ |
| hbase.zookeeper.quorum              | (none)        | Yes      | Zookeeper cluster address                                    |
| hbase.zookeeper.property.clientPort | (none)        | Yes      | Zookeeper client port                                        |
| hbase.zookeeper.znode.parent        | /hbase        | No       | Zookeeper znode parent of hbase                                   |
| hbase.client.retries.number         | 3             | No       | Retry times to connect to hbase client                       |
| hbase.client.pause.time             | 100           | No       | HBase client disconnect time                                 |
| hbase.rpc.protection.enable         | false         | No       | Communication privacy protection. You can get this from `hbase-site.xml`. |
| hbase.default.value                 | NULL          | No       | The default value of data in table                           |
| hbase.metastore.type                | hetuMetastore | No       | The storage of hbase metadata, you can choose `hetuMetastore` |
| hbase.authentication.type           | (none)        | No       | Access security authentication mode of hbase component  |
| hbase.kerberos.principal            | (none)        | No       | User name for security authentication                        |
| hbase.kerberos.keytab               | (none)        | No       | Key for security authentication                              |
| hbase.client.side.enable            | false         | No       | Access data in clientSide mode and obtain data in the region based on snapshots in HDFS.|
| hbase.hbase.site.path               | (none)        | No       | Configuration used to connect to a secure hbase cluster      |
| hbase.client.side.snapshot.retry    | 100           | No       | Number of snapshot create retry times on the HBase Client|
| hbase.hdfs.site.path                | (none)        | No       | The path of hdfs-site.xml for connecting to the HDFS cluster in ClientSide mode|
| hbase.core.site.path                | (none)        | No       | The path of core-site.xml for connecting to the HDFS cluster in ClientSide mode|
| hbase.jaas.conf.path                | (none)        | No       | Jaas for security authentication                             |
| hbase.krb5.conf.path                | (none)        | No       | Krb5 for security authentication                             |


## Table Properties

| Property         | Type    | Default Value                  | Required | Description                                                  |
| ---------------- | ------- | ------------------------------ | -------- | ------------------------------------------------------------ |
| column_mapping   | String  | All columns in the same family | No       | Specify the mapping relationship between column families and column qualifiers in the table. If you need to link a table in the hbase data source, the column_mapping information must be consistent with the hbase data source; if you create a new table that does not exist on the hbase data source, the column_mapping is specified by the user. |
| row_id           | String  | The first column name          | No       | row_id is the column name corresponding to rowkey in the hbase table |
| hbase_table_name | String  | null                           | No       | hbase_table_name specifies the tablespace and table name on the hbase data source to be linked, use ":" connect tablespace and table name, the default tablespace is "default". |
| external         | Boolean | true                           | No       | If external is true, it means that the table is a mapping table of the table in the hbase data source. It does not support deleting the original table on the hbase data source; if external is false, the table on hbase data source will be deleted at the same time as the local hbase table is deleted. |
| split_by_char| String| 0~9,a~z,A~Z| No| split_by_char is the basis for sharding, if the first character of rowkey consists of digits, sharding can be performed based on different digits to improve concurrency. Different types of symbols are separated by commas. If this parameter is set incorrectly, the query result may be incomplete. Set it based on the actual rowkey.|


## Data Types

Currently, following 10 data type are supported:  VARCHAR, TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, BOOLEAN, TIME, DATE and TIMESTAMP.



## Push Down

The HBase Connector supports push down most of operators, such as rowkey-based point query, rowkey-based range query. Besides, those predicate conditions are supported to push down: `=`, `>=`, `>`, `<`, `<=`, `!=`, `in`, `not in`, `between and`. 


## Performance Optimization Configuration
```
1. Specify sharding rules during table creation to improve the query performance of a single table. Create table xxx() with(split_by_char='0-9, a~z, A~Z'),
split_by_char indicates the range of the first character of a rowKey, which is the basis for splitting a segment. If the first character of the RowKey consists of digits, the rowKey can be segmented based on different digits to improve the query concurrency. Different types of symbols are separated by commas.
If this parameter is set incorrectly, the query result may be incomplete. Set this parameter based on the actual situation. If there is no special requirement, you do not need to change the value. By default, all digits and letters are contained.
If rowKey is a Chinese character, create table xxx() with(split_by_char='一~锯').
In addition, the splitKey of the pre-partition is specified based on split_by_char during table creation and data is distributed to each region as much as possible. In this way, performance is improved during HBase read/write.

2. The client side mode can be configured to read data, improving the multi-concurrent query performance.
The working mechanism of ClientSide is to create snapshots for HBase tables in HDFS and record the address of the region where each data file is located. When data is read, the region is directly accessed without going through the HBase region server. This reduces the pressure on the region server in high concurrency.

Add the following configuration to the etc/catalog/hbase.properties file:
    hbase.client.side.enable=true
    hbase.core.site.path=/opt/openlookeng/xxx/core-site.xml
    hbase.hdfs.site.path=/opt/openlookeng/xxx/hdfs-site.xml

[If HDFS is a security cluster, add the following configuration:
    hbase.authentication.type=KERBEROS
    hbase.krb5.conf.path=/opt/openlookeng/xxx/krb5.conf
    hbase.kerberos.keytab=/opt/openlookeng/xxx/user.keytab
    hbase.kerberos.principal=lk_username@HADOOP.COM

Note: Currently, the snapshot lifecycle in client side mode is not maintained. If the number of snapshots exceeds the limit of HBase, you need to manually clear the snapshots in HDFS.
```


## Usage Examples

The HBase Connector basically supports all SQL statements, includes creating, querying, and deleting schemas; Adding, dropping, and modifying tables; Inserting data, deleting rows and so on. 

Here are some example:

### Create Schema

```sql
CREATE SCHEMA schemaName;
```



### Drop Schema

It can only drop empty schema. 

```sql
DROP SCHEMA schemaName;
```



### Create Table

HBase Connector supports two forms of table creation: 

1. Create a table and directly link to a existing table in the HBase data source, you must specify hbase_table_name.

2. Create a new table that does not exist in the HBase data source, there is no need to specify hbase_table_name. We must specify 'external = false'.


Below is an example of how to create a table `schemaName.tableName` and link it to an existing table named `hbaseNamespace:hbaseTable` :

Column mapping format: 'column_name:family:qualifier'

```sql
CREATE TABLE schemaName.tableName (
    rowId		VARCHAR,
    qualifier1	TINYINT,
    qualifier2	SMALLINT,
    qualifier3	INTEGER,
    qualifier4	BIGINT,
    qualifier5	DOUBLE,
    qualifier6	BOOLEAN,
    qualifier7	TIME,
    qualifier8	DATE,
    qualifier9	TIMESTAMP
)
WITH (
    column_mapping = 'qualifier1:f1:q1, qualifier2:f1:q2, 
    qualifier3:f2:q3, qualifier4:f2:q4, qualifier5:f2:q5, qualifier6:f3:q1, 
    qualifier7:f3:q2, qualifier8:f3:q3, qualifier9:f3:q4',
    row_id = 'rowId',
    hbase_table_name = 'hbaseNamespace:hbaseTable',
    external = false
);
```

**Note**

If user specifies `hbase_table_name`, then the `schemaName` must be same as the namespace in `hbase_table_name` when creating a table. Otherwise, the creation will be failed. 

Example (**failure to create table**):

```sql
CREATE TABLE default.typeMapping (
    rowId 		VARCHAR,
    qualifier1 	INTEGER
)
WITH (
    column_mapping = 'qualifier2:f1:q2',
    row_id = 'rowId',
    hbase_table_name = 'hello:type4'
);
```

When creating a table to link with HBase cluster, you must ensure that the schema name in HBase connector and HBase data source are the same and achieve the same isolation level.



### Drop Table

```sql
DROP TABLE schemaName.tableName;
```



### Rename Table

```sql
ALTER TABLE schemaName.tableName RENAME TO schemaName.tableNameNew;
```

**Note**

*Currently, it does not support renaming the tables which are created through HBase.*



### Add Column

```sql
ALTER TABLE schemaName.tableName
ADD COLUMN column_name INTEGER 
WITH (family = 'f1', qualifier = 'q1');
```

**Note**

*Currently, deleting columns is not supported*



### Delete row/rows

```sql
DELETE FROM schemeName.tableName where rowId = xxx;
DELETE FROM schemeName.tableName where qualifier1 = xxx;
DELETE FROM schemeName.tableName;
```



## Limitations

Statement `show tables` can only display the tables that the user has established the associations with HBase data source. Because HBase does not provide an interface to retrieve metadata of tables.
