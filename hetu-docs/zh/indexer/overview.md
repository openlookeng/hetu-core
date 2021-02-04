
# openLooKeng启发式索引

## 简介

可以在数据库表的一个或多个列创建索引，从而提供更快的随机查找。大多数大数据格式，如ORC、Parquet和CarbonData，都已经内置了索引。

启发式索引允许在现有数据上创建索引，但将索引存储在外部的的原始数据源中。这样做好处如下：

  - 索引对底层数据源不可知，并且可由任何查询引擎使用
  - 无需重写现有数据文件即可对现有数据进行索引
  - 可以创建底层数据源不支持的新索引类型
  - 索引数据不占用数据源存储空间

## 使用场景

**当前，启发式索引支持ORC存储格式的hive数据源。**

### 1.查询过程中过滤预定分片

支持的索引：Bloom, BTree, MinMax

当引擎需要调度一个TableScan操作时，它可以调度worker节点上的Split。这些Split负责读取部分源数据。但是如果应用了谓词，则并非所有Split都会返回数据。

例如，`select * from test_base where j1='070299439'`

通过为谓词列保留外部索引，启发式索引可以确定每个Split是否包含正在搜索的值，并且只对可能包含该值的Split安排读操作。

![indexer_filter_splits](../images/indexer_filter_splits.png)

### 2.读取ORC文件时提前筛选Stripes

支持的索引：Bloom, MinMax

与分片过滤类似，当使用Hive Connector读取ORC文件时，Stripe可以被提前过滤来减少读取的数据量，从而提升查询性能。

### 3.读取ORC文件时筛选行

支持的索引：Bitmap

当需要从ORC文件中读取数据时，如果有一个谓词存在，那么就不需要批量读取中的所有行。

通过为谓词列保留外部位图索引，将实现只读取匹配当行，来提升内存和处理器表现。在服务器高并发时提升尤其明显。

## 示例教程

这一教程将通过一个示例查询语句来展示索引的用法。完整的配置请参见[Properties](../admin/properties.md)。

### 1. 配置索引

在 `etc/config.properties` 中加入这些行：

    hetu.heuristicindex.filter.enabled=true
    hetu.heuristicindex.filter.cache.max-memory=10GB
    hetu.heuristicindex.indexstore.uri=/opt/hetu/indices
    hetu.heuristicindex.indexstore.filesystem.profile=index-store-profile

路径配置白名单：["/tmp", "/opt/hetu", "/opt/openlookeng", "/etc/hetu", "/etc/openlookeng", 工作目录]

避免选择根目录；路径不能包含../；如果配置了node.data_dir,那么当前工作目录为node.data_dir的父目录；如果没有配置，那么当前工作目录为openlookeng server的目录

**注意**：
- `LOCAL` 本地文件系统将*不再*被支持
- `HDFS` 应用于生产环境来在集群中共享数据。
- 所有节点必须有相同的文件系统配置。
- 在服务器运行中可以通过`set session heuristicindex_filter_enabled=false;`关闭启发式索引。

然后在`etc/filesystem/index-store-profile.properties`中创建一个HDFS描述文件, 其中`index-store-profile`是上面使用的名字：

    fs.client.type=hdfs
    hdfs.config.resources=/path/to/core-site.xml,/path/to/hdfs-site.xml
    hdfs.authentication.type=NONE
    fs.hdfs.impl.disable.cache=true
    
如果HDFS集群开启了KERBEROS验证，还需要配置`hdfs.krb5.conf.path, hdfs.krb5.keytab.path, hdfs.krb5.principal`. 

### 2. 确定索引建立的列

对于这样的语句：

    SELECT * FROM table1 WHERE id="abcd1234";
   
如果id比较唯一，bloom索引可以大大较少读取的分段数量。

在本教程中我们将以这个语句为例。

### 3. 创建索引

要创建索引, 在命令行中输入：

    CREATE INDEX index_name USING bloom ON table1 (column);
    
### 4. 运行语句

完成上面的操作后，再次在Hetu服务器运行这个语句，服务器将在后台自动加载索引。接下来的语句将会从中获得性能提升。

## 索引配置属性

| 属性名称                                            | 默认值               | 是否必填| 说明|
|---------------------------------------------------|---------------------|-------|----------|
| hetu.heuristicindex.filter.enabled                | false               | 否    | 启用启发式索引|
| hetu.heuristicindex.filter.cache.max-memory       | 10GB                | 否    | 索引缓存大小|
| hetu.heuristicindex.filter.cache.soft-reference   | true                | 否    | 允许GC在内存不足时从缓存中清除内容来释放内存|
| hetu.heuristicindex.filter.cache.ttl              | 24h                 | 否    | 索引缓存的有效时间|
| hetu.heuristicindex.filter.cache.load-threads     | 10                  | 否    | 从存储文件系统并行加载索引文件使用的线程数|
| hetu.heuristicindex.filter.cache.loading-delay    | 10s                 | 否    | 在异步加载索引到缓存前等待的时长|
| hetu.heuristicindex.indexstore.uri                | /opt/hetu/indices/  | 否    | 所有索引文件存储的目录|
| hetu.heuristicindex.indexstore.filesystem.profile | local-config-default| 否    | 用于存储索引文件的文件系统属性描述文件名称|

索引功能现使用Hetu Metastore管理元数据。请参阅 [Hetu Metastore](../admin/meta-store.md) 获取关于如何配置的更多信息。

## 索引语句

参见 [Heuristic Index Statements](./hindex-statements.md).

-----

## 支持的索引类型

| 索引 ID | 过滤类型  | 最适用的列                           | 支持的运算符           | 注释                    | 用例                                                                                                                                                                                                          |
|----------|-----------------|--------------------------------------------|---------------------------------------|---------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Bloom](./bloom.md)    | Split<br>Stripe | 大量不同数据值<br>(如ID) | `=` `IN`                                  |                           | `create index idx using bloom on hive.hindex.users (id);`<br>`select name from hive.hindex.users where id=123`                                                                                                    |
| [Btree](./btree.md)    | Split           | 大量不同数据值<br>(如ID) | `=` `>` `>=` `<` `<=` `IN` `BETWEEN` | 表必须被分区 | `create index idx using btree on hive.hindex.users (id) where regionkey IN (1,4) with ("level"='partition')`<br>(假设表根据regionkey分区)<br>`select name from hive.hindex.users where id>123` |
| [MinMax](./minmax.md)   | Split<br>Stripe | 列数据被排序            | `=` `>` `>=` `<` `<=` |                           | `create index idx using bloom on hive.hindex.users (age);`<br>(假设数据根据年龄已排序)<br>`select name from hive.hindex.users where age>25`                                                              |
| [Bitmap](./bitmap.md)   | Row             | 少量不同数据值<br>(如性别) | `=` `IN`                                  |                           | `create index idx using bitmap on hive.hindex.users (gender);`<br>`select name from hive.hindex.users where gender='female'`                                                                                      |

**注意:** 包含不支持的运算符的语句依然会正常运行，但是不会从启发式索引中获得性能提升。


## 选择索引类型

启发式索引用于根据谓词表达式过滤数据。请根据下面的决策流程图选择适用于数据列的最佳索引。

Cardinality 是指数据集中值域的大小。例如，`ID`列通常有很大的cardinality，
而`employeeType`列通常cardinality很小(如 Manager, Developer, Tester)。

![index-decision](../images/index-decision.png)

用例:

1. `SELECT id FROM employees WHERE site = 'lab';`

    在这个语句中`site`的cardinality很小（没有很多不同的地点取值)。 因此，**Bitmap索引**比较适合。

2. `SELECT * FROM visited WHERE id = '34857' AND date < '2020-01-01';`

    在这个语句中`id`有很高的cardinality （每一个ID是唯一的)。同时，表根据`date`已经分区。因此**Btree索引**比较适合。
    
3. `SELECT * FROM salaries WHERE salary > 50251.40;`

    在这个语句中`salary`有很高的cardinality（每个员工的收入总有些许不同)。假设表已经根据`salary`排序, 则**MinMax索引**最为适合。

4. `SELECT * FROM assets WHERE id = 50;`

    在这个语句中`id`有很高的cardinality （每一个ID是唯一的)。但是，表没有分区。因此**Bloom索引**比较适合。

5. `SELECT * FROM phoneRecords WHERE phone='1234567890' and type = 'outgoing' and date > '2020-01-01';`

    在这个语句中`phone`有很高的cardinality (即使有重复的电话，绝大部分号码总是不同的), `type`的cardinality较低 (只有两种：呼出/呼入),
    同时数据根据`date`已分区。因此，在`phone`上创建**Btree索引**并在`type`上创建**Bitmap索引**最为适合。

## 添加自定义的索引类型

参见 [Adding your own Index Type](./new-index.md).

## 权限控制

参见 [Built-in System Access Control](../security/built-in-system-access-control.md).
