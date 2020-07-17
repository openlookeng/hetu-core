
# CarbonData连接器


## 概述
CarbonData连接器支持查询存储在CarbonData仓库中的数据。CarbonData由三个组件组成：

- CarbonData存储格式的数据文件，一般存储在Hadoop分布式文件系统（HDFS）中。
- 该元数据仅用于表和列模式验证。carbondata元数据与数据文件存储在一起，通过Hive Metastore Service (HMS)访问。
- 一种称为HiveQL/SparkSQL的查询语言。这种查询语言在分布式计算框架（如MapReduce或Spark）上执行。

openLooKeng只使用前两个组件：数据和元数据。它不使用HiveQL/SparkSQL或Hive的任何部分执行环境。

**说明：** *openLooKeng支持CarbonData 2.0.1。*

## 配置

Carbondata连接器支持Apache Hadoop 2.x及以上版本。

用以下内容创建`etc/catalog/carbondata.properties`，以将`carbondata`连接器挂载为`carbondata`目录，将`example.net:9083`替换为Hive元存储 Thrift服务的正确主机和端口：

```properties
connector.name=carbondata
hive.metastore.uri=thrift://example.net:9083
```

### HDFS配置

对于基本设置，openLooKeng自动配置HDFS客户端，不需要任何配置文件。在某些情况下，例如使用联邦HDFS或NameNode高可用性时，需要指定额外的HDFS客户端选项，以便访问HDFS集群。要指定选项，添加`hive.config.resources`属性来引用HDFS配置文件：

``` properties
hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml,/etc/hadoop/conf/yarn-site.xml,/etc/hadoop/conf/mapred-site.xml
```

如果设置需要，只指定附加的配置文件。同时建议减少配置文件，使其具有所需的最小属性集，因为附加属性可能导致问题。

所有openLooKeng节点上必须存在配置文件。如果用户正在引用现有的Hadoop配置文件，请确保将其复制到任何没有运行Hadoop的openLooKeng节点。

### HDFS用户名和权限

在对CarbonData表执行任何`CREATE TABLE`或`CREATE TABLE AS`语句之前，openLooKeng应该能够访问Hive和HDFS。Hive的仓库目录由`hive-site.xml`中的配置变量`hive.metastore.warehouse.dir`指定，默认值为`/user/hive/warehouse`。

在不使用带HDFS的Kerberos时，openLooKeng会使用openLooKeng进程的操作系统用户来访问HDFS。例如，如果openLooKeng作为`nobody`运行，则openLooKeng将作为`nobody`访问HDFS。可以通过在openLooKeng [JVM配置](../installation/deployment.md#jvm配置)中设置`HADOOP_USER_NAME`系统属性来覆盖此用户名，用适当的用户名替换`hdfs_user`：

``` properties
-DHADOOP_USER_NAME=hdfs_user
```

`hive`用户通常可行，因为Hive通常随`hive`用户启动，并且该用户可以访问Hive仓库。

无论何时修改openLooKeng访问HDFS的用户时，请移除HDFS上的`/tmp/openlookeng-*`，`/tmp/presto-*`，`/tmp/hetu-*`，因为新用户可能无法访问现有的临时目录。

### 访问Kerberos身份验证保护的Hadoop集群

HDFS和Hive元存储都支持Kerberos身份验证。但是，目前还不支持通过票据缓存进行Kerberos身份验证。

CarbonData连接器安全需要的属性在[CarbonData配置属性](./carbondata.md#carbondata配置属性)表中列出。有关安全选项的更详细的讨论，请参阅[Hive安全配置](./hive-security.md)部分。

## Carbondata配置属性

| 属性名称| 说明| 默认值|
|----------|:----------|----------|
| carbondata.store-location| CarbonData仓库的存储位置。如果不指定，则使用默认的Hive仓库路径，即 */user/hive/warehouse/**carbon.store*** | `${hive.metastore.warehouse.dir} /carbon.store`|
| `hive.metastore`| 要使用的Hive元存储的类型。openLooKeng目前支持默认的Hive Thrift元存储（`thrift`）。| `thrift`|
| `hive.config.resources`| 以逗号分隔的HDFS配置文件列表。这些文件必须存在于运行openLooKeng的机器上。示例：`/etc/hdfs-site.xml`| 
| `hive.hdfs.authentication.type`| HDFS身份验证类型。取值为`NONE`或`KERBEROS`。| `NONE`|
| `hive.hdfs.impersonation.enabled`| 启用HDFS端用户模拟。| `false`||
| `hive.hdfs.presto.principal`| openLooKeng在连接到HDFS时将使用的Kerberos主体。| |
| `hive.hdfs.presto.keytab`| HDFS客户端keytab位置。| |
| `hive.collect-column-statistics-on-write`| 启用写入时自动收集列级统计信息。详见[表统计](./hive.md#表统计信息)。| `true`|

## Hive Thrift元存储配置属性说明

| 属性名称| 说明|
|----------|----------|
| `hive.metastore.uri`| 使用Thrift协议连接Hive元存储的URI。如果提供了多个URI，则默认使用第一个URI，其余URI为回退元存储。该属性必选。示例：`thrift://192.0.2.3:9083`或`thrift://192.0.2.3:9083,thrift://192.0.2.4:9083`|
| `hive.metastore.username`| openLooKeng用于访问Hive元存储的用户名。|
| `hive.metastore.authentication.type`| Hive元存储身份验证类型。取值为`NONE`或`KERBEROS`（默认为`NONE`）。|
| `hive.metastore.service.principal`| Hive元存储服务的Kerberos主体。|
| `hive.metastore.client.principal`| openLooKeng在连接到Hive元存储服务时将使用的Kerberos主体。|
| `hive.metastore.client.keytab`| Hive元存储客户端keytab位置。|

## 表统计信息

CarbonData连接器在写入数据时，总是收集基本的统计信息（`numFiles`、`numRows`、`rawDataSize`、`totalSize`），默认还会收集列级统计信息：

| 列类型| Null数量| 不同值数量| 最小值/最大值|
|----------|----------|----------|----------|
| `SMALLINT`完成| Y| Y| Y|
| `INTEGER`完成| Y| Y| Y|
| `BIGINT`完成| Y| Y| Y|
| `DOUBLE`完成| Y| Y| Y|
| `REAL`完成| Y| Y| Y|
| `DECIMAL`完成| Y| Y| Y|
| `DATE`完成| Y| Y| Y|
| `TIMESTAMP`完成| Y| Y| N|
| `VARCHAR`完成| Y| Y| N|
| `CHAR`完成| Y| Y| N|
| `VARBINARY`完成| Y| N| N|
| `BOOLEAN`完成| Y| Y| N|

## 示例

CarbonData连接器支持查询和操作CarbonData表和模式（数据库）。大部分操作可以使用openLooKeng，而一些不常用的操作则需要直接使用Spark::Carbondata。

### Create Table

创建新表`orders`：

```sql
CREATE TABLE orders (
    orderkey	bigint,
   	orderstatus varchar,
	totalprice 	double,
    orderdate	varchar
);
```

支持的属性

| 属性| 说明| 默认值|
|----------|----------|----------|
| location| 用于存放表数据的目录。<br />如果不指定，将使用默认的文件系统位置。| 可选|

在指定位置创建新表`orders`：

```sql
CREATE TABLE orders_with_store_location (
    orderkey 	bigint,
    orderstatus	varchar,
    totalprice	double,
    orderdate	varchar
 )
WITH ( location = '/store/path' );
```

**说明：**

- *如果路径不是完全限定域名，则会存储在默认文件系统中。*

### Create Table as Select

基于SELECT语句的输出创建新表

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

将额外的行加载到`orders`表中：

```sql
INSERT INTO orders
VALUES (BIGINT '101', 'Ready', 1000.25, '2020-06-08');
```

通过覆盖现有行将额外的行加载到`orders`表中：

```sql
INSERT OVERWRITE orders 
VALUES (BIGINT '101', 'Delivered', 1040.25, '2020-06-26');
```

将额外的行与来自其他表的值加载到`orders`表中；

```sql
INSERT INTO orders 
SELECT * FROM delivered_orders;
```

### Update

更新表`orders`的所有行：

```sql
UPDATE orders SET orderstatus = 'Ready';
```

使用过滤条件更新`orders`：

```sql
UPDATE orders SET orderstatus = 'Delivered'
WHERE totalprice >1000 AND totalprice < 2000;
```

### Delete

删除表`orders`中所有行：

```sql
DELETE FROM orders;
```

使用过滤条件的表`orders`：

```sql
DELETE FROM orders WHERE orderstatus = 'Not Available';
```

### Drop Table

删除已存在的表。

```sql
DROP TABLE orders;
```

## Carbondata连接器限制

CarbonData连接器暂不支持如下操作：

- 不支持`CREATE TABLE`、`sort_by`、`bucketed_by`和`partitioned_by`表属性。
- 不支持物化视图。
- 不支持数组、列表和映射等复杂数据类型。
- 不支持更改表使用。
- 不支持对分区表的操作。