+++
weight = 5
title = "HBase"
+++

# HBase连接器

------


## 概述

HBase连接支持在外部Apache HBase实例上查询和创建表。用户可以在HBase连接器中创建表，并映射到HBase Cluster中已有的表，支持insert、select和delete操作。

HBase连接器维护着一个元存储，用于持久化HBase元数据，目前元存储只支持以下存储格式：`Local File System`、`Hadoop Distributed File System (HDFS)`和`openLooKeng Metastore`。

**注意：** *Hbase连接器支持Apache HBase 1.3.1版本。*

## 连接器配置

创建`etc/catalog/hbase.properties`，将HBase连接器挂载为HBase目录，并按需要替换`hbase.xxx`属性：

```properties
connector.name=hbase-connector

hbase.zookeeper.quorum=xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx

hbase.zookeeper.property.clientPort=xxxx

hbase.metastore.type=local

hbase.metastore.uri=/xxx/hbasemetastore.ini
```

对于`hbase.zookeeper.quorum`的值，如果有多个IP地址，请使用逗号（`,`）作为分隔符。

**注意**

如果使用`Local File System`和`HDFS`作为元存储，则需要创建一个空文件，如`hbasemetastore.ini`。

**使用HDFS存储HBase元数据**

需要添加以下属性：

```properties
hbase.core.site.path=/xxx/core-site.xml

hbase.hdfs.site.path=/xxx/hdfs-site.xml

hbase.metastore.type=hdfs

hbase.metastore.uri=hdfs://xxx.xxx.xxx.xxx:21088/xxx/hbasemetastore.ini
```

**使用openLooKeng元存储来存储HBase元数据**

必须创建`etc/hetu-metastore.properties`来连接数据库。具体配置请参见[VDM](vdm.html)连接器。将以下属性添加到配置文件：

```properties
hbase.metastore.type=hetuMetastore
```

**Kerberos配置：**

```properties
hbase.jaas.conf.path=/xxx/jaas.conf

hbase.hbase.site.path=/xxx/hbase-site.xml

hbase.krb5.conf.path=/xxx/krb5.conf

hbase.kerberos.keytab=/xxx/user.keytab

hbase.kerberos.principal=xxx

hbase.authentication.type=KERBEROS
```

## 配置属性

| 属性名称| 默认值| 是否必填| 说明|
|----------|----------|----------|----------|
| hbase.zookeeper.quorum| （无）| 是| ZooKeeper集群地址|
| hbase.zookeeper.property.clientPort| （无）| 是| Zookeeper客户端端口。|
| hbase.client.retries.number| 3| 否| HBase客户端连接重试次数|
| hbase.client.pause.time| 100| 否| HBase客户端断连时间|
| hbase.rpc.protection.enable| false| 否| 通信隐私保护。可以从`hbase-site.xml`获取该属性的值。|
| hbase.default.value| NULL| 否| 表中数据的默认值|
| hbase.metastore.type| local| 否| HBase元数据的存储，可以从`local/hdfs/hetuMetastore`中选择一种|
| hbase.metastore.uri| （无）| 是| 存储HBase元数据的文件路径|
| hbase.core.site.path| （无）| 否| 连接HDFS的配置文件|
| hbase.hdfs.site.path| （无）| 否| 连接HDFS的配置文件|
| hbase.authentication.type| （无）| 否| HDFS/HBase组件访问安全身份验证方式|
| hbase.kerberos.principal| （无）| 否| 安全身份验证的用户名|
| hbase.kerberos.keytab| （无）| 否| 安全身份验证的密钥|
| hbase.hbase.site.path| （无）| 否| 连接安全HBase集群的配置|
| hbase.jaas.conf.path| （无）| 否| 安全身份验证的JAAS|
| hbase.krb5.conf.path| （无）| 否| 安全身份验证的krb5|

## 表属性

| 属性| 类型| 默认值| 是否必填| 说明|
|----------|----------|----------|----------|----------|
| column\_mapping| String| 同一个族中的所有列| 否| 指定表中列族与列限定符的映射关系。如果需要链接HBase数据源中的表，则column\_mapping信息必须与HBase数据源一致；如果创建一个HBase数据源中不存在的新表，则column\_mapping由用户指定。|
| row\_id| String| 第一个列名| 否| row\_id为HBase表中RowKey对应的列名|
| hbase\_table\_name| String| NULL| 否| hbase\_table\_name指定要链接的HBase数据源上的表空间和表名，使用“:”连接表空间和表名，默认表空间为“default”。|
| external| Boolean| true| 否| 如果external为true，表示该表是HBase数据源中表的映射表。不支持删除HBase数据源上原有的表。当external为false时，删除本地HBase表的同时也会删除HBase数据源上的表。|

## 数据说明

目前支持10种数据类型：  VARCHAR、TINYINT、SMALLINT、INTEGER、BIGINT、DOUBLE、BOOLEAN、TIME、DATE和TIMESTAMP。

## 下推

HBase连接器支持下推大部分运算符，如基于RowKey的点查询、基于RowKey的范围查询等。此外，还支持这些谓词条件以进行下推：`=`、`>=`、`>`、`<`、`<=`、`!=`、`in`、`not in`、`is null`、`is not null`、`between and`。

## 使用示例

HBase连接器基本上支持所有的SQL语句，包括创建、查询、删除模式，添加、删除、修改表，插入数据，删除行等。

以下是一些示例：

### 创建模式

```sql
CREATE SCHEMA schemaName;
```

### 删除模式

只支持删除空模式。

```sql
DROP SCHEMA schemaName;
```

### 创建表

HBase连接器支持两种建表形式：

1. 创建表并直接链接到HBase数据源中已存在的表。

2. 创建HBase数据源中不存在的新表。

以下示例创建表`schemaName.tableName`并链接到一个名为`hbaseNamespace:hbaseTable`的现有表：

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

**注意**

如果用户指定了`hbase_table_name`，则创建表时，`schemaName`必须和`hbase_table_name`中的命名空间一致。否则将会创建失败。

示例（**建表失败**）：

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

创建连接HBase集群的表时，必须保证HBase连接器和HBase数据源中的模式名称一致，且达到相同的隔离级别。

### 删除表

```sql
DROP TABLE schemaName.tableName;
```

### 重命名表

```sql
ALTER TABLE schemaName.tableName RENAME TO schemaName.tableNameNew;
```

**注意**

*目前不支持对通过HBase创建的表进行重命名。*

### 添加列

```sql
ALTER TABLE schemaName.tableName
ADD COLUMN column_name INTEGER 
WITH (family = 'f1', qualifier = 'q1');
```

**注意**

*目前不支持删除列。*

### 删除行

```sql
DELETE FROM schemeName.tableName where rowId = xxx;
DELETE FROM schemeName.tableName where qualifier1 = xxx;
DELETE FROM schemeName.tableName;
```

## 限制

语句`show tables`只能显示用户已与HBase数据源建立关联的表，因为HBase没有提供接口来检索表的元数据。