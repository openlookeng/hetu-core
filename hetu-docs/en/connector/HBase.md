# HBase Connector

------

## Overview

The HBase Connector allows querying and creating tables on an external Apache HBase instance. Users can create a table in HBase connector, mapping it to an existing table in HBase Cluster, and support insert, select, or delete.

The HBase Connector maintains a Metastore to persist HBase metadata, currently support Metastore: `openLooKeng Metastore`.

**Note:** *We use Apache HBase 2.2.3 version in HBase connector*



## Connector Configuration

Create `etc/catalog/hbase.properties` to mount the HBase connector as the HBase catalog, replacing the `hbase.xxx` properties as required:

```properties
connector.name=hbase-connector

hbase.zookeeper.quorum=xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx

hbase.zookeeper.property.clientPort=xxxx

hbase.metastore.type=hetuMetastore
```

For the value of `hbase.zookeeper.quorum`, please use comma (`,`) as the delimiter if it has multiple ip addresses. 

**Note**

**Use openLooKeng Metastore to store HBase metadata**

You have to create `etc/hetu-metastore.properties` to connect database. For the details of configuration, please refer to [VDM Connector](./vdm.md). Adding below property to the configuration file:

```properties
hbase.metastore.type=hetuMetastore
```

## Configuration Properties

| Property Name                       | Default Value | Required | Description                                                  |
| ----------------------------------- | ------------- | -------- | ------------------------------------------------------------ |
| hbase.zookeeper.quorum              | (none)        | Yes      | Zookeeper cluster address                                    |
| hbase.zookeeper.property.clientPort | (none)        | Yes      | Zookeeper client port                                        |
| hbase.client.retries.number         | 3             | No       | Retry times to connect to hbase client                       |
| hbase.client.pause.time             | 100           | No       | HBase client disconnect time                                 |
| hbase.rpc.protection.enable         | false         | No       | Communication privacy protection. You can get this from `hbase-site.xml`. |
| hbase.default.value                 | NULL          | No       | The default value of data in table                           |
| hbase.metastore.type                | hetuMetastore | No       | The storage of hbase metadata, you can choose `hetuMetastore` |


## Table Properties

| Property         | Type    | Default Value                  | Required | Description                                                  |
| ---------------- | ------- | ------------------------------ | -------- | ------------------------------------------------------------ |
| column_mapping   | String  | All columns in the same family | No       | Specify the mapping relationship between column families and column qualifiers in the table. If you need to link a table in the hbase data source, the column_mapping information must be consistent with the hbase data source; if you create a new table that does not exist on the hbase data source, the column_mapping is specified by the user. |
| row_id           | String  | The first column name          | No       | row_id is the column name corresponding to rowkey in the hbase table |
| hbase_table_name | String  | null                           | No       | hbase_table_name specifies the tablespace and table name on the hbase data source to be linked, use ":" connect tablespace and table name, the default tablespace is "default". |
| external         | Boolean | true                           | No       | If external is true, it means that the table is a mapping table of the table in the hbase data source. It does not support deleting the original table on the hbase data source; if external is false, the table on hbase data source will be deleted at the same time as the local hbase table is deleted. |



## Data Types

Currently, following 10 data type are supported:  VARCHAR, TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, BOOLEAN, TIME, DATE and TIMESTAMP.



## Push Down

The HBase Connector supports push down most of operators, such as rowkey-based point query, rowkey-based range query. Besides, those predicate conditions are supported to push down: `=`, `>=`, `>`, `<`, `<=`, `!=`, `in`, `not in`, `is null`, `is not null`, `between and`. 



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

1. Create a table and directly link to a existing table in the HBase data source.

2. Create a new table that does not exist in the HBase data source.


Below is an example of how to create a table `schemaName.tableName` and link it to an existing table named `hbaseNamespace:hbaseTable` :

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
    column_mapping = 'rowId:f:rowId, qualifier1:f1:q1, qualifier2:f1:q2, 
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
    column_mapping = 'rowId:f:rowId, qualifier2:f1:q2',
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
