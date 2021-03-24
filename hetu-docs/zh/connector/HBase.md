
# HBase连接器

------


## 概述

HBase连接支持在外部Apache HBase实例上查询和创建表。用户可以在HBase连接器中创建表，并映射到HBase Cluster中已有的表，支持insert、select和delete操作。

HBase连接器维护着一个元存储，用于持久化HBase元数据，目前元存储只支持以下存储格式：`openLooKeng Metastore`。

**注意：** *Hbase连接器仅支持连接Apache HBase 2.2.3及以下的版本。*

## 连接器配置

1 . 创建并编辑`etc/catalog/hbase.properties`，按需要替换下面`xxx`的值；connector.name是固定的；对于`hbase.zookeeper.quorum`的值，如果有多个IP地址，请使用逗号(`,`)作为分隔符。

```properties
connector.name=hbase-connector
hbase.zookeeper.quorum=xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx
hbase.zookeeper.property.clientPort=xxxx
```

2 . 使用openLooKeng元存储来存储HBase元数据

必须创建`etc/hetu-metastore.properties`来配置元数据存储平台，存储平台可选择RDBMS/HDFS。

配置举例：

创建并编辑 etc/hetu-metastore.properties

选择一：hetu-metastore配置为RDBMS：
```
hetu.metastore.type=jdbc
hetu.metastore.db.url=jdbc:mysql://xx.xx.xx.xx:3306/dbName....
hetu.metastore.db.user=root
hetu.metastore.db.password=123456
```

选择二：hetu-metastore配置为HDFS：
```properties
hetu.metastore.type=hetufilesystem
# profile name of hetu file system
hetu.metastore.hetufilesystem.profile-name=hdfs-config-metastore
#the path of metastore storage in the hetu file system
hetu.metastore.hetufilesystem.path=/etc/openlookeng/metastore
```    
如果选择HDFS，接下来需要配置hetu filesystem（配置文件名称同上述profile-name保持一致，后缀为properties）。

创建并编辑 etc/filesystem/hdfs-config-metastore.properties
```properties
fs.client.type=hdfs
hdfs.config.resources=/opt/openlookeng/xxx/core-site.xml,/opt/openlookeng/xxx/hdfs-site.xml
hdfs.authentication.type=NONE
```    
hdfs kerberos相关配置请参见[filesystem](../develop/filesystem.md)

**注意：** *以上hbase.properties和hetu-metastore.properties两个配置文件缺一不可。如配置为HDFS，则另外需要hdfs-config-metastore.properties*

```
可能遇到的问题：配置完成后，启动服务遇到“xxx路径（A配置文件）不在白名单列表”的错误
解决方案：选择日志中打印的白名单列表的一个目录，将所报错的路径下的文件，拷贝到该目录,并修改A配置文件中相应路径。
```

**Kerberos配置：**

如果HBase/Zookeeper是安全集群，则需要配置kerberos相关信息

```properties
hbase.jaas.conf.path=/opt/openlookeng/xxx/jaas.conf

hbase.hbase.site.path=/opt/openlookeng/xxx/hbase-site.xml

hbase.krb5.conf.path=/opt/openlookeng/xxx/krb5.conf

hbase.kerberos.keytab=/opt/openlookeng/xxx/user.keytab

hbase.kerberos.principal=lk_username@HADOOP.COM

hbase.authentication.type=KERBEROS
```

编辑 jaas.conf
```properties
Client {
com.sun.security.auth.module.Krb5LoginModule required
useKeyTab=true
keyTab="/opt/openlookeng/xxx/user.keytab"
principal="lk_username@HADOOP.COM"
useTicketCache=false
storeKey=true
debug=true;
};
```

## 配置属性

| 属性名称| 默认值| 是否必填| 说明|
|----------|----------|----------|----------|
| hbase.zookeeper.quorum| （无）| 是| ZooKeeper集群地址|
| hbase.zookeeper.property.clientPort| （无）| 是| Zookeeper客户端端口|
| hbase.zookeeper.znode.parent| /hbase| 否| HBase的Zookeeper根节点路径|
| hbase.client.retries.number| 3| 否| HBase客户端连接重试次数|
| hbase.client.pause.time| 100| 否| HBase客户端断连时间|
| hbase.rpc.protection.enable| false| 否| 通信隐私保护。可以从`hbase-site.xml`获取该属性的值|
| hbase.default.value| NULL| 否| 表中数据的默认值|
| hbase.metastore.type| hetuMetastore| 否| HBase元数据的存储，`hetuMetastore`|
| hbase.authentication.type| （无）| 否| HDFS/HBase组件访问安全身份验证方式|
| hbase.kerberos.principal| （无）| 否| 安全身份验证的用户名|
| hbase.kerberos.keytab| （无）| 否| 安全身份验证的密钥|
| hbase.client.side.enable| false| 否| 以clientSide模式访问数据，根据hdfs上的快照，获取region上的数据|
| hbase.hbase.site.path| （无）| 否| 连接安全HBase集群的配置|
| hbase.client.side.snapshot.retry| 100| 否| HBase客户端创建snapshot的重试次数|
| hbase.hdfs.site.path| （无）| 否| 配置ClientSide模式时，连接HDFS集群的配置hdfs-site.xml的路径|
| hbase.core.site.path| （无）| 否| 配置ClientSide模式时，连接HDFS集群的配置core-site.xml的路径|
| hbase.jaas.conf.path| （无）| 否| 安全身份验证的JAAS|
| hbase.krb5.conf.path| （无）| 否| 安全身份验证的krb5|


## 表属性

| 属性| 类型| 默认值| 是否必填| 说明|
|----------|----------|----------|----------|----------|
| column\_mapping| String| 同一个族中的所有列| 否| 指定表中列族与列限定符的映射关系。如果需要链接HBase数据源中的表，则column\_mapping信息必须与HBase数据源一致；如果创建一个HBase数据源中不存在的新表，则column\_mapping由用户指定。|
| row\_id| String| 第一个列名| 否| row\_id为HBase表中RowKey对应的列名|
| hbase\_table\_name| String| NULL| 否| hbase\_table\_name指定要链接的HBase数据源上的表空间和表名，使用“:”连接表空间和表名，默认表空间为“default”。|
| external| Boolean| true| 否| 如果external为true，表示该表是HBase数据源中表的映射表。不支持删除HBase数据源上原有的表。当external为false时，删除本地HBase表的同时也会删除HBase数据源上的表。|
| split\_by\_char| String| 0~9,a~z,A~Z| 否| split\_by\_char为分片切割的依据，若RowKey的第一个字符由数字构成，则可以根据不同的数字进行分片切割，提高查询并发度。不同类型的符号用逗号隔开。如果设置不当，会导致查询数据结果不完整，请根据RowKey的实际情况进行配置。|

## 数据说明

目前支持10种数据类型：  VARCHAR、TINYINT、SMALLINT、INTEGER、BIGINT、DOUBLE、BOOLEAN、TIME、DATE和TIMESTAMP。

## 下推

HBase连接器支持下推大部分运算符，如基于RowKey的点查询、基于RowKey的范围查询等。此外，还支持这些谓词条件以进行下推：`=`、`>=`、`>`、`<`、`<=`、`!=`、`in`、`not in`、`between and`、`is null`、`is not null`。

## 性能优化配置
```
1. 建表时指定分片切割规则，提升单表查询性能；
Create table xxx() with(split_by_char='0~9,a~z,A~Z')，
split_by_char表示rowKey的第一个字符的范围，为分片切割的依据。若RowKey的第一个字符由数字构成，则可以根据不同的数字进行分片切割，提高查询并发度。不同类型的符号用逗号隔开。
如果设置不当，会导致查询数据结果不完整，请根据RowKey的实际情况进行配置。无特殊要求时，无需修改。默认情况已包含所有的数字和字母。
若rowKey为汉字，则create table xxx() with(split_by_char='一~锯')；
另外，建表时会根据split_by_char指定预分区的splitKey，尽量将数据分散到各个region，那样在进行hbase读写时，对性能会有很好的改善。

2. 可配置ClientSide模式来读取数据,提升多并发查询性能；
ClientSide的工作机制是在HDFS上创建hbase表的snapshot，记录各个数据文件所在的region地址，在读取数据时，不需要经过hbase region server，而是直接访问region，这样可以在高并发下降低region server的压力。
    
编辑 etc/catalog/hbase.properties 新增如下配置
   hbase.client.side.enable=true
   hbase.core.site.path=/opt/openlookeng/xxx/core-site.xml
   hbase.hdfs.site.path=/opt/openlookeng/xxx/hdfs-site.xml
   
【HDFS为安全集群，则另外新增如下配置】
   hbase.authentication.type=KERBEROS
   hbase.krb5.conf.path=/opt/openlookeng/xxx/krb5.conf
   hbase.kerberos.keytab=/opt/openlookeng/xxx/user.keytab
   hbase.kerberos.principal=lk_username@HADOOP.COM
   
备注：
1. clientSide模式的snapshot生命周期目前是没有进行维护的，如果超出了hbase对快照数的限制，则需要手动清理hdfs上的快照。
2. clientSide模式下，不支持算子下推。
3. 不允许为hbase的系统表创建快照（如schema名为hbase）
```

## 使用示例

HBase连接器基本上支持所有的SQL语句，包括创建、查询、删除模式，添加、删除、修改表，插入数据，删除行等。

以下是一些示例：

### 创建模式

```sql
CREATE SCHEMA schemaName;
```

### 删除模式

只支持删除空的模式。

```sql
DROP SCHEMA schemaName;
```

### 创建表

HBase连接器支持两种建表形式：

1. 创建表并直接链接到HBase数据源中已存在的表，必须指定hbase_table_name。

2. 创建HBase数据源中不存在的新表，不需要指定hbase_table_name。我们必须指定‘external = false’。

以下示例创建表`schemaName.tableName`并链接到一个名为`hbaseNamespace:hbaseTable`的现有表：

映射关系的格式为：'column_name:family:qualifier'

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

**注意**

如果用户指定了`hbase_table_name`，则创建表时，`schemaName`必须和`hbase_table_name`中的命名空间一致。否则将会创建失败。

示例（**建表失败**）：

```sql
CREATE TABLE default.typeMapping (
    rowId 		VARCHAR,
    qualifier1 	INTEGER
)
WITH (
    column_mapping = 'qualifier1:f1:q2',
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

当使用openlk插入大量的数据到hbase中时，插入成功与否取决于hbase server的处理能力。当插入数据时出现错误时，建议增大hbase server的相关参数。我们不建议使用openlk导hbase数据，而是使用第三方工具（Bulk）。

vi hbase-site.xml
```
<!-- default is 2 -->
  <property>
    <name>hbase.hregion.memstore.block.multiplier</name>
    <value>4</value>
  </property>

  <!-- default is 64MB 67108864 -->
  <property>
    <name>hbase.hregion.memstore.flush.size</name>
    <value>134217728</value>
  </property>
  <!-- default is 7, should be at least 2x compactionThreshold -->
  <property>
    <name>hbase.hstore.blockingStoreFiles</name>
    <value>200</value>
  </property>
  <property>
    <name>hbase.hstore.compaction.max</name>
    <value>20</value>
    <description>Max number of HStoreFiles to compact per 'minor' compaction.</description>
  </property>
```