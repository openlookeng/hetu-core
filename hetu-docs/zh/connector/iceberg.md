# iceberg连接器
==============

## 概述

openLooKeng Iceberg 是一种用于大型分析数据集的开放表式。Iceberg 连接器允许查询存储在以 Iceberg 格式编写的文件中的数据。
Iceberg 数据文件可以存储为 Parquet、ORC 格式，由format表定义中的属性决定。该表format默认为ORC。

## 要求

要使用 Iceberg，您需要：

- 从openLooKeng 协调节点和工作节点到分布式对象存储的网络访问。
- 访问 Hive 元存储服务 (HMS) 。
- 从 openLooKeng 协调器到 HMS 的网络访问。使用 Thrift 协议的 Hive Metastore 访问默认使用端口 9083。

## Hive元存储目录

Hive 元存储目录是默认实现。使用它时，Iceberg 连接器支持与 Hive 连接器相同的元存储配置属性。至少，hive.metastore.uri必须配置。

```properties
connector.name=iceberg
hive.metastore.uri=thrift://localhost:9083
```

## 一般配置

这些配置属性与使用的目录实现无关。

Iceberg 通用配置属性
用以下内容创建`etc/catalog/iceberg.properties`，请把`localhost:9083`替换为Hive元存储Thrift服务的正确主机和端口：

| 属性名                    | 属性值                     | 描述                                   |
|:-----------------------|:------------------------|:-------------------------------------|
|       connector.name                 | iceberg                 | 连接名                              |
|       hive.metastore.uri    | thrift://localhost:9083 | hive连接地址                             |
|       iceberg.file-format              | ORC                     | 为Iceberg表定义数据存储文件格式。可能的值是PARQUET、ORC |

## SQL 支持

此连接器提供对 Iceberg 中的数据和元数据的读取访问和写入访问。除了全局可用和读取操作语句外，连接器还支持以下功能：

### 表相关语句
- 创建
```sql
CREATE TABLE ordersg (
    order_id BIGINT,
    order_date DATE,
    account_number BIGINT,
    customer VARCHAR,
    country VARCHAR)
WITH (partitioning = ARRAY['month(order_date)', 'bucket(account_number, 10)', 'country']);
```
### 列相关语句
- 新增
```sql
alter table ordersg add COLUMN zip varchar;
```
- 重命名
```sql
ALTER TABLE ordersg RENAME COLUMN zip TO zip_r;
```
- 删除
```sql
ALTER TABLE ordersg DROP COLUMN zip_r;
```
### 数据相关语句
- 插入
```sql
insert into ordersg values(1,date'1988-11-09',666888,'tim','US');
```
- 删除
```sql
Delete from ordersg where order_id = 1;
```
- 更新
```sql
update ordersg set customer = 'Alice' where order_id = 2;
```
- 查询
```sql
select * from ordersg; 
```

## 分区表

Iceberg 通过在表列上指定转换来支持分区。为转换生成的每个唯一元组值创建一个分区。身份转换只是列名。其他变换是：

| 转换                      | 描述                                             |
|:------------------------|:-----------------------------------------------|
| year(ts) | 每年都会创建一个分区。分区值是1970年1月1日和1970 年1月1日之间的整数差。     |
| month(ts) | 每年的每个月都会创建一个分区。分区值是1970年1月1日和1970 年1月1日之间的整数差。 |
| day(ts)  |  每年的每一天创建一个分区。分区值是1970年1月1日和1970年 1月1日之间的整数差。  |
| hour(ts) |    每天每小时创建一个分区。分区值是分钟和秒设置为零的时间戳。  |
| bucket(x,nbuckets)     |  数据被散列到指定数量的桶中。分区值是 的整数散列x，值介于 0 和 0 之间。nbuckets-1 |
| truncate(s,nchars)     |  分区值是 的第一个nchars字符s。 |

在此示例中，创建表并按照order_date表的月份、order_date的散列（具有 10 个存储桶），进行分区
```sql
CREATE TABLE ordersg (
                        order_id BIGINT,
                        order_date DATE,
                        account_number BIGINT,
                        customer VARCHAR,
                        country VARCHAR)
  WITH (partitioning = ARRAY['month(order_date)', 'bucket(account_number, 10)', 'country']);
```
手动修改分区
```sql
ALTER TABLE ordersg SET PROPERTIES partitioning = ARRAY['month(order_date)'];
```

按分区删除，对于分区表，如果WHERE子句仅在标识转换的分区列上指定过滤器，Iceberg 连接器支持删除整个分区，这可以匹配整个分区。鉴于上面的表定义，此 SQL 将删除所有分区country：US

```sql
DELETE FROM iceberg.testdb.ordersg WHERE country = 'US';
```

## 版本查询回溯

Iceberg 支持数据的“快照”模型，可以通过查询语句查询任意历史版本的数据，其中表快照由快照 ID 标识。

连接器为每个 Iceberg 表提供了一个系统快照表。快照由 BIGINT 快照 ID 标识。customer_orders您可以通过运行以下命令找到表的最新快照 ID ：
```sql
SELECT snapshot_id FROM "ordersg $snapshots" ORDER BY committed_at DESC LIMIT 10;
```
|snapshot_id|
|:----------|
|921254093881523606|
|535467754709887442|
|343895437069940394|
|34i302849038590348|
|(4 rows)|

SQL 过程system.rollback_to_snapshot允许调用者将表的状态回滚到之前的快照 ID：
```sql
CALL iceberg.system.rollback_to_snapshot('testdb', 'ordersg', 8954597067493422955);
```

## 元数据表

连接器为每个 Iceberg 表公开了几个元数据表。这些元数据表包含有关 Iceberg 表内部结构的信息。您可以通过将元数据表名称附加到表名称来查询每个元数据表：

```sql
SELECT * FROM "ordersg$data";
```

### $data表#

该$data表是 Iceberg 表本身的别名。

该命名：
```sql
SELECT * FROM "ordersg$data";
```
相当于：
```sql
SELECT * FROM ordersg;
```

### $properties 表

该$properties表提供了对有关 Iceberg 表配置的一般信息以及该表标记的任何其他元数据键/值对的访问。

您可以使用以下查询检索Iceberg 表的当前快照的属性：
```sql
SELECT * FROM "ordersg$properties";
```

key                   | value    |
-----------------------+----------+
write.format.default   | PARQUET  |


### $history表
该$history表提供了对 Iceberg 表执行的元数据更改的日志。

您可以使用以下查询检索 Iceberg 表的更改日志：

```sql
SELECT * FROM "ordersg$history";
```

made_current_at                  | snapshot_id          | parent_id            | is_current_ancestor
----------------------------------+----------------------+----------------------+--------------------
2022-08-19 05:42:37.854 UTC  |   7464177163099901858  | 7924870943332311497   |  true
2022-08-19 05:44:35.212 UTC  |   2228056193383591891  | 7464177163099901858  |  true

查询的输出具有以下列：

历史专栏

|姓名	| 类型                          |	描述|
|:----------------------------|:----------------------------|:----------------------|
|made_current_at| 	timestamp(3)with time zone |	快照激活的时间|
|snapshot_id| 	bigint                     |	快照的标识符|
|parent_id| 	bigint	                    |父快照的标识符|
|is_current_ancestor| 	boolean	                   |此快照是否是当前快照的祖先|

### $snapshots 表

该$snapshots表提供了 Iceberg 表快照的详细视图。快照由一个或多个文件清单组成，完整的表内容由这些清单中所有数据文件的并集表示。

您可以使用以下查询检索有关 Iceberg 表的快照的信息 ：

```sql
SELECT * FROM "ordersg$snapshots";
```

|   committed_at   |   snapshot_id   |   parent_id   |   operation   |   manifest_list   |   summary   |
|   2022-08-08 08:20:04.126 UTC   |   7026585913702073835   |              |   append   |   hdfs://hadoop1:9000/home/gitama/hadoop/hive/user/hive/warehouse/test_100.db/orders08/metadata/snap-7026585913702073835-1-d0b5ba3d-6363-4f32-974e-79bb68d19423.avro   |   {changed-partition-count=0, total-equality-deletes=0, total-position-deletes=0, total-delete-files=0, total-files-size=0, total-records=0, total-data-files=0}   |
|   2022-08-08 08:21:58.343 UTC   |   629134202395791160   |   7026585913702073835   |   append   |   hdfs://hadoop1:9000/home/gitama/hadoop/hive/user/hive/warehouse/test_100.db/orders08/metadata/snap-629134202395791160-1-b6e9c1c3-0532-4bf8-a814-a159494e272d.avro   |   {changed-partition-count=1, added-data-files=1, total-equality-deletes=0, added-records=1, total-position-deletes=0, added-files-size=289, total-delete-files=0, total-files-size=289, total-records=1, total-data-files=1}   |

查询的输出具有以下列：
快照列

|姓名	| 类型                           | 	描述                                                                                                           |
|:----------|:-----------------------------|:--------------------------------------------------------------------------------------------------------------|
|committed_at	| timestamp(3) with time zone	 | 快照激活的时间                                                                                                       |
|snapshot_id| 	bigint	                     | 快照的标识符                                                                                                        |
|parent_id	|bigint	| 父快照的标识符                      |
|operation	| varchar                      | 	在 Iceberg 表上执行的操作类型。Iceberg 中支持的操作类型有：- append添加新数据时 -replace当文件被删除和替换而不更改表中的数据时  -overwrite添加新数据以覆盖现有数据时  -delete当从表中删除数据并且没有添加新数据时 |
|manifest_list| 	varchar	                    | 包含有关快照更改的详细信息的 avro 清单文件列表。                                                                                   |
|summary	| map(varchar, varchar)	       | 从上一个快照到当前快照所做更改的摘要                                                                                            |

### $manifests表#

该$manifests表提供了与 Iceberg 表日志中执行的快照相对应的清单的详细概述。
您可以使用以下查询检索有关 Iceberg 表的清单的信息 ：

```sql
SELECT * FROM "ordersg$manifests";
```

Path   |   length   |   partition_spec_id   |   added_snapshot_id  |  added_data_files_count   |   existing_data_files_count   |   deleted_data_files_count     |   partitions              
---------------------------------------+---------------------+---------------------+---------------------+-------------------+--------------------+--------------------+--------------------+--------------------+-------------------
hdfs://hadoop1:9000/home/gitama/hadoop/hive/user/hive/warehouse/test_100.db/orders08/metadata/b6e9c1c3-0532-4bf8-a814-a159494e272d-m0.avro  |  6534  |  0  | 629134202395791160 | 1 | 0 | 0 | [ ]

查询的输出具有以下列：
清单列

|名称| 	类型	           |描述|
|:----|:---------------------|:--------|
|path| 	varchar	         |清单文件位置|
|length	| bigint	        |清单文件长度|
|partition_spec_id| 	integer             |	用于写入清单文件的分区规范的标识符|
|added_snapshot_id	| bigint          |	添加此清单条目的快照的标识符|
|added_data_files_count| 	integer	      |ADDED清单文件中具有状态的数据文件的数量|
|existing_data_files_count	| integer	   |EXISTING清单文件中具有状态的数据文件的数量|
|deleted_data_files_count	| integer	  |DELETED清单文件中具有状态的数据文件的数量|
|partitions| 	array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar)) |	分区范围元数据|


### $partitions表
该$partitions表提供了 Iceberg 表的分区的详细概述。
您可以使用以下查询检索有关 Iceberg 表分区的信息 ：

```sql
SELECT * FROM "ordersg$partitions";
```

|   record_count   |   file_count   |   total_size   |   data   |   --------------+-------------+---------------------+---------------------+------------------------------------|   1   |   1   |   289   |   {id={min=null, max=null, null_count=0, nan_count=null}, name={min=null, max=null, null_count=0, nan_count=null}}   |

查询的输出具有以下列：

分区列

|姓名| 	类型                                                                    |	描述|
|:-----|:-----------------------------------------------------------------------|:-------|
|record_count	| bigint	                                                                |分区中的记录数|
|file_count| 	bigint                                                                |	分区中映射的文件数|
|total_size| 	bigint                                                                |	分区中所有文件的大小|
|data	| row(... row (min ..., max ... , null_count bigint, nan_count bigint))	 |分区范围元数据|

### $files表

该$files表提供了 Iceberg 表当前快照中数据文件的详细概述。
要检索有关 Iceberg 表的数据文件的信息，请使用以下查询：

```sql
SELECT * FROM "ordersg$files";
```

| content   |   file_path   |   file_format   |   record_count   |   file_size_in_bytes   |   column_sizes   |   value_counts   |   null_value_counts   |   nan_value_counts   |   lower_bounds   |   upper_bounds   |   key_metadata   |   split_offsets   |   equality_ids   |   
|  0   |   hdfs://192.168.31.120:9000/user/hive/warehouse/orders19/data/20220819_034313_39152_vdmku-1709db2a-dc6f-4ef9-bb77-23f4c150801f.orc   |   ORC   |   1   |   354   |      |   {1=1, 2=1, 3=1}","{1=0, 2=0, 3=0}   |      |      |      |      |      |      |   
|   0   |   hdfs://192.168.31.120:9000/user/hive/warehouse/orders19/data/20220819_054009_11365_xq568-1803130c-6b7b-4da6-b460-dfb44f176ef4.orc   |   ORC   |   1   |   413   |      |   {1=1, 2=1, 3=1, 4=1}   |   {1=0, 2=0, 3=0, 4=1}   |      |      |      |      |      |      |

查询的输出具有以下列：

文件列

|姓名	|类型|	描述|
|:---------|:----------|:-------------|
|content	|integer|	存储在文件中的内容类型。Iceberg 中支持的内容类型有： -DATA(0) - POSITION_DELETES(1) -EQUALITY_DELETES(2)|
|file_path|	varchar|	数据文件位置|
|file_format|	varchar	|数据文件的格式|
|record_count|	bigint|	数据文件中包含的条目数|
|file_size_in_bytes|	bigint	|数据文件大小|
|column_sizes|	map(integer, bigint)|	Iceberg 列 ID 与其在文件中对应的大小之间的映射|
|value_counts|	map(integer, bigint)	|Iceberg 列 ID 与其对应的文件中条目数之间的映射|
|null_value_counts|	map(integer,bigint)	|Iceberg 列 ID 与其NULL在文件中对应的值计数之间的映射|
|nan_value_counts|	map(integer,bigint)|	Iceberg 列 ID 与其对应的文件中非数值计数之间的映射|
|lower_bounds|	map(integer,bigint)|	Iceberg 列 ID 与其在文件中对应的下限之间的映射|
|upper_bounds|	map(integer,bigint)|	Iceberg 列 ID 与其在文件中对应的上界之间的映射|
|key_metadata|	varbinary	|有关用于加密此文件的加密密钥的元数据（如果适用）|
|split_offsets|	array(bigint)|	推荐拆分位置列表|
|equality_ids|	array(integer)	|相等删除文件中用于相等比较的字段 ID 集|


## 更改表执行

连接器支持以下用于ALTER TABLE EXECUTE的命令(具体请见文件合并)。

## 文件合并

该optimize命令用于重写指定表的活动内容，以便将其合并为更少但更大的文件。在表被分区的情况下，数据压缩分别作用于每个选择进行优化的分区，此操作提高了读取性能。

合并所有大小低于可选file_size_threshold参数（阈值的默认值为100MB）的文件：
```sql
ALTER TABLE ordersg EXECUTE optimize;
```
以下语句合并表中小于 10 兆字节的文件：
```sql
ALTER TABLE ordersg EXECUTE optimize(file_size_threshold => '10MB');
```
## 更改表集属性

连接器支持使用ALTER TABLE SET PROPERTIES修改现有表的属性。

创建表后可以更新以下表属性：
- format
- partitioning

```sql
ALTER TABLE ordersg SET PROPERTIES format ='PARQUET';
```

或者可以将该列设置country为表上的分区列：
```sql
ALTER TABLE ordersg SET PROPERTIES partitioning = ARRAY[<existing partition columns>, 'country'];
```

可以使用SHOW CREATE TABLE ordersg 显示表属性的当前值。









