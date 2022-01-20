
# 内存连接器

内存连接器将所有数据和元数据存储在工作节点上的内存中来获得更好的性能。
同时元数据和数据也被后台写入磁盘并在需要时自动读取。

## 配置

### 内存连接器配置

配置内存连接器时，创建或修改具有以下内容的目录属性文件`etc/catalog/memory.properties`：

``` properties
connector.name=memory
memory.max-data-per-node=200GB
memory.spill-path=/opt/hetu/data/spill    
```

#### 其他必要的配置
- 本节将介绍使用内存连接器所需的其他配置。
- 更多信息请参考[Hetu Metastore](../admin/meta-store.md)和[State Store](../admin/state-store.md)的文档。
  ##### 单个节点设置
  - 本节将给出单节点集群上内存连接器的示例配置。
  - 创建文件 `etc/catalog/memory.properties`并填入以下配置:
  ``` properties
  connector.name=memory
  memory.max-data-per-node=200GB
  memory.spill-path=/opt/hetu/data/spill
  ```
  - 创建文件 `etc/hetu-metastore.properties` 并填入以下配置:
  ```properties
  hetu.metastore.type=hetufilesystem
  hetu.metastore.hetufilesystem.profile-name=default
  hetu.metastore.hetufilesystem.path=/tmp/hetu/metastore
  hetu.metastore.cache.type=local
  ```
  ##### 多节点设置
  - 本节将为具有多个节点的集群提供内存连接器的示例配置。
  - 创建文件 `etc/catalog/memory.properties` 并填入以下配置:
  ``` properties
  connector.name=memory
  memory.max-data-per-node=200GB
  memory.spill-path=/opt/hetu/data/spill
  ```
  - 在 `etc/config.properties` 文件中加入以下代码来启用 State Store:
    - State Store 允许 Memory Connector 自动清理删除的表，否则只有在创建另一个表时才会清理表。
  ```properties
  hetu.embedded-state-store.enabled=true
  ```
  - 创建文件 `etc/state-store.properties` 并填入以下配置:
  ```properties
  state-store.type=hazelcast
  state-store.name=test
  state-store.cluster=test-cluster
  hazelcast.discovery.mode=tcp-ip
  hazelcast.discovery.port=7980
  # 每个服务器的ip地址和hazelcast端口应该被声明在这里。
  # 格式：`hazelcast.discovery.tcp-ip.seeds=host1:port,host2:port` 
  hazelcast.discovery.tcp-ip.seeds=host1:7980, host2:7980
  ```
  - 创建文件 `etc/hetu-metastore.properties` 并填入以下配置:
  ```properties
  hetu.metastore.type=hetufilesystem
  hetu.metastore.hetufilesystem.profile-name=hdfs
  hetu.metastore.hetufilesystem.path=/tmp/hetu/metastore
  # 确认使用全局缓存!
  hetu.metastore.cache.type=global
  ```
  - 创建文件 `etc/filesystem/hdfs.properties`  使openLooKeng使用相应的文件系统:
  ```properties
  fs.client.type=hdfs
  # Path to hdfs resource files (e.g. core-site.xml, hdfs-site.xml)
  hdfs.config.resources=/tmp/hetu/hdfs-site.xml
  # hdfs authentication, accepted values: KERBEROS/NONE
  hdfs.authentication.type=NONE
  ```

**提示：**
- `spill-path`必须设置为一个有充足存储空间的路径。推荐使用SSD以获得更好性能。 可以依照需求自定义。 
- 关于更多详细信息与其他可选配置项，请参见**配置属性** 章节。
- 在`etc/config.properties`中，请确保`task.writer-count`的数字不小于配置的openLooKeng集群的节点个数。这会帮助把所有数据更均匀地分配到各个节点上。
## 示例

使用内存连接器创建表：

    CREATE TABLE memory.default.nation AS
    SELECT * from tpch.tiny.nation;

在内存连接器中向表中插入数据：

    INSERT INTO memory.default.nation
    SELECT * FROM tpch.tiny.nation;

从Memory连接器中选择：

    SELECT * FROM memory.default.nation;

删除表：

    DROP TABLE memory.default.nation;

使用内存连接器创建表，同时排序、索引和配置数据写入磁盘压缩:

    CREATE TABLE memory.default.nation
    WITH (
        sorted_by=array['nationkey'],
        partitioned_by=array['regionkey'],
        index_columns=array['name'],
        spill_compression=true
    )
    AS SELECT * from tpch.tiny.nation;

内存连接器会在后台自动排序数据并对`memory.default.nation`表创建索引。完成后针对对应列的查询会显著变快。

目前，`sorted_by`和`partitioned_by`,仅支持对一列数据排序。

## 使用JMX的内存和磁盘使用情况
JMX可用于显示内存连接器表的内存和磁盘使用情况
设置请参考[JMX Connector](./jmx.md)

`jmx.current` 的 `io.prestosql.plugin.memory.data:name=MemoryTableManager` 表包含所有表的内存和磁盘使用大小的信息，以字节为单位

    SELECT * FROM jmx.current."io.prestosql.plugin.memory.data:name=MemoryTableManager";

```
 currentbytes | alltablesdiskbyteusage | alltablesmemorybyteusage |   node   |                       object_name                       
--------------+------------------------+--------------------------+----------+---------------------------------------------------------
           23 |                   3456 |                       23 | example1 | io.prestosql.plugin.memory.data:name=MemoryTableManager 
          253 |                   8713 |                      667 | example2 | io.prestosql.plugin.memory.data:name=MemoryTableManager 
```

并非所有表都在内存中，因为它们可能会溢出到磁盘中。`currentbytes`列将显示当前内存中的表占用的当前内存。

每个节点的使用情况显示为单独的一行，可以使用聚合函数来显示整个集群的总使用情况。例如，要查看所有节点上的总磁盘或内存使用情况，请运行：

    SELECT sum(alltablesdiskbyteusage) as totaldiskbyteusage, sum(alltablesmemorybyteusage) as totalmemorybyteusage FROM jmx.current."io.prestosql.plugin.memory.data:name=MemoryTableManager";

```
totaldiskbyteusage | totalmemorybyteusage
-------------------+---------------------
             12169 |                  690
```

## 配置属性

| 属性名称                         | 默认值   | 是否必要 | 描述               |
|---------------------------------------|-----------------|---------|---------------------------|
| `memory.spill-path                   `  | None          | Yes     | 在磁盘上持久化数据的位置。优先使用位于SSD的路径 |
| `memory.max-data-per-node            `  | 256MB         | Yes     | 每个节点的内存大小使用限制 |
| `memory.max-logical-part-size        `  | 256MB         | No      | 每个逻辑分片(LogicalPart)的大小限制 |
| `memory.max-page-size                `  | 1MB           | No      | 每个Page的大小限制 |
| `memory.logical-part-processing-delay`  | 5s            | No      | 表创建后建立索引和写入磁盘前的等待时间 |
| `memory.thread-pool-size             `  | Half of threads available to the JVM | No      | 后台线程（排序，清理数据，写入磁盘等）使用的线程池大小 |
| `memory.table-statistics-enabled`       | False         | No      | 启用后，用户可以运行分析来收集统计信息并利用该信息来加速查询。|


路径配置白名单：["/tmp", "/opt/hetu", "/opt/openlookeng", "/etc/hetu", "/etc/openlookeng", 工作目录]

## 在创建表时的额外WITH属性

使用这些属性来创建表以使得查询更快：

| 属性名称                  | 属性类型                   | 是否必要                          | 描述        |
|--------------------------|---------------------------|----------------------------------|------------       |
| sorted_by                | `array['col']`            | 最多一个列，列数据必须是可比较的     | 排序并对该列创建索引 |
| partitioned_by           | `array['col']`            |最多一个列                        | 在给定的列上对表进行分区 |
| index_columns            | `array['col1', 'col2']`   | None                             | 在该列上创建索引|
| spill_compression        | `boolean`                 | None                             | 在磁盘上持久化数据时是否启用压缩 |


## 索引类型
内存连接器支持使用索引来加速某些算子的执行速度。支持的索引类型如下：

| Index ID     | 是否可用于`sorted_by`或`index_columns`   | 支持的运算符                           |
|--------------|-----------------------------------------|---------------------------------------|
| Bloom        | 仅`index_columns`                                 | `=` `IN`                             |                   
| MinMax       | 两者都可                           | `=` `>` `>=` `<` `<=` `IN` `BETWEEN` |
| Sparse       | 仅`sorted_by`                           | `=` `>` `>=` `<` `<=` `IN` `BETWEEN` |

使用统计信息
-----------------
如果开启了统计配置，可以参考下面的例子来使用。

使用内存连接器创建一个表：

    CREATE TABLE memory.default.nation AS
    SELECT * from tpch.tiny.nation;

运行ANALYZE以收集统计信息：

    ANALYZE memory.default.nation;

然后运行查询。请注意，目前我们不支持自动统计更新，因此如果表更新，您将需要再次运行 ANALYZE。

## 开发者信息

下图展示了内存连接器的总体设计：

![Memory Connector Overall Design](../images/memory-connector-design.png)


### 调度流程

要处理的数据存储在page中，这些page被分发到openLooKeng中的不同工作节点(worker)。
在内存连接器中，每个worker都有若干个LogicalParts。
在表创建期间，worker中的LogicalParts以循环方式根据page内容被填充。
数据内容也将作为后台进程的一部分自动持久化到磁盘。
如果没有足够的内存来保存整个数据，则依据LRU规则从内存中释放最近不使用的表。 
HetuMetastore用于持久化表元数据。在查询时，当调度Tablescan操作时，将调度查询LogicalParts。

### 逻辑分片（LogicalPart）

如设计图的下半部分所示，LogicalPart是包含索引和原始数据内容的数据结构。
作为表创建后的后台处理的一部分，数据在每一个逻辑分片中被排序和创建索引，以实现更快的查询，但在处理过程中已插入的数据仍然是可查询的。 
LogicalParts 具有最大可配置大小（默认为 256 MB）。一旦前一个逻辑部分已满，就会创建新的逻辑部分。

### 索引

LogicalPart 中创建了布隆过滤器、稀疏索引和 MinMax 索引。
基于下推的predicate，可以使用布隆过滤器和 MinMax 索引过滤掉整个 LogicalParts。
进一步的页面过滤是使用稀疏索引完成的。
首先对页面进行排序，然后进行优化，最后创建一个稀疏索引。
稀疏索引不会记录所有的数据值而只会间隔抽取一些。这使得索引更小。
稀疏索引有助于减少输入行，但不能执行完美的过滤。
进一步的过滤是由 openLooKeng 的 Filter Operator 完成的。
参考上面的稀疏索引示例，这是内存连接器为不同查询过滤数据的方式：


```
查询: column=a.
返回 Page 0 和 1.

查询: column=b.
返回 Page 2 和 Page 1.

查询: column=c.
返回小于等于c的Page 2.

查询: column=d.
不会返回任何Page。因为d的上一个索引值为b（page 2），而该page的最后一个值为c。

对于包含> >= < <= BETWEEN IN算子的条件，也参照上面的逻辑。
```

## 内存连接器限制和已知问题

- 如果没有 State Store 和带有全局缓存的 Hetu Metastore，在 `DROP TABLE` 之后，内存不会立即释放到 worker 上。它将在下一个“CREATE TABLE”操作时被释放。
- 当前`sorted_by`只支持按一个列排序。
- 如果一个CTAS (CREATE TABLE AS)查询失败或被取消，一个无效的表的记录会留在系统中。该表将需要被手动删除。
- 我们支持 BOOLEAN、所有 INT 类型、CHAR、VARCHAR、DOUBLE、REAL、DECIMAL、DATE、TIME、UUID 类型作为分区键。

