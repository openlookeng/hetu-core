
# 内存连接器

内存连接器将所有数据和元数据存储在工作节点上的内存中来获得更好性能。同时元数据和数据也被后台写入磁盘并在需要时自动读取。

## 配置

### 内存连接器配置

要配置内存连接器，创建一个具有以下内容的目录属性文件`etc/catalog/memory.properties`：

``` properties
connector.name=memory
memory.splits-per-node=10
memory.max-data-per-node=200GB
memory.spill-path=/opt/data/spill    
```

**提示：**
- `splits-per-node` 须小于节点上处理器核心的数量，并且必须*在所有节点上都相同*！
- `spill-path`必须设置为一个有充足存储空间的路径。推荐使用SSD以获得更好性能。 可以依照需求自定义。 
- 关于更多详细信息与其他可选配置项，请参见**配置属性** 章节。
- 在`etc/config.properties`中，请确保`task.writer-count`的数字不小于配置的openLooKeng集群的节点个数。这会帮助把所有数据更均匀地分配到各个节点上。
- Hetu Metastore必须被妥善配置来保证内存连接器的正常功能。请参阅[Hetu Metastore](../admin/meta-store.md)。

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
        index_columns=array['name', 'regionkey'],
        spill_compression=true
    )
    AS SELECT * from tpch.tiny.nation;

内存连接器会在后台自动排序数据并对`memory.default.nation`表创建索引。完成后针对对应列的查询会显著变快。

目前，`sorted_by`仅支持对一列数据排序。

## 配置属性

| 属性名称                         | 默认值   | 是否必要 | 描述               |
|---------------------------------------|-----------------|---------|---------------------------|
| `memory.splits-per-node              `  | None          | Yes     | 每个节点的并行split个数 |
| `memory.spill-path                   `  | None          | Yes     | 在磁盘上持久化数据的位置。优先使用位于SSD的路径 |
| `memory.max-data-per-node            `  | 256MB         | Yes     | 每个节点的内存大小使用限制 |
| `memory.max-logical-part-size        `  | 256MB         | No      | 每个逻辑分片(LogicalPart)的大小限制 |
| `memory.max-page-size                `  | 1MB           | No      | 每个Page的大小限制 |
| `memory.logical-part-processing-delay`  | 5s            | No      | 表创建后建立索引和写入磁盘前的等待时间 |
| `memory.thread-pool-size             `  | Half of threads available to the JVM | No      | 后台线程（排序，清理数据，写入磁盘等）使用的线程池大小 |

## 在创建表时的额外WITH属性

使用这些属性来创建表以使得查询更快：

| 属性名称                  | 属性类型                   | 是否必要                          | 描述        |
|--------------------------|---------------------------|----------------------------------|------------       |
| sorted_by                | `array['col']`            | 最多一个列，列数据必须是可比较的     | 排序并对该列创建索引 |
| index_columns            | `array['col1', 'col2']`   | None                             | 在该列上创建索引|
| spill_compression        | `boolean`                 | None                             | 在磁盘上持久化数据时是否启用压缩 |

## 索引类型
内存连接器支持使用索引来加速某些算子的执行速度。支持的索引类型如下：

| Index ID     | 是否可用于`sorted_by`或`index_columns`   | 支持的运算符                           |
|--------------|-----------------------------------------|---------------------------------------|
| Bloom        | 两者都可                                 | `=` `IN`                             |                   
| MinMax       | 仅`sorted_by`                           | `=` `>` `>=` `<` `<=` `IN` `BETWEEN` |
| Sparse       | 仅`sorted_by`                           | `=` `>` `>=` `<` `<=` `IN` `BETWEEN` |

## 开发者信息

下图展示了内存连接器的总体设计：

![Memory Connector Overall Design](../images/memory-connector-overall-design.png)

数据被切分为分片（Split）分布到各个worker节点上。这些分片又被组合为逻辑分片（LogicalPart）。每个逻辑分片包含索引和数据。表数据会自动写入到磁盘。如果没有足够的内存来保存整个表是，则可以从内存中释放表。数据在额外的后台进程中进行排序和索引，从而可以更快地创建表。该表在此期间仍可查询处理，但查询效率不高（索引尚未启用）。Hetu Metastore用于持久化表元数据。

### 分片（Split）

在表创建期间，page被分发给每个worker。每个worker将有相同数量的分片。分片以循环方式填充page。当调度Table Scan时，将在每一个分片上调度查询。将拆分数设置为小于节点上内核数的值可实现最大并行度。并非所有表都能完整放在内存中，因此在内存限制被达到时引擎会依据LRU规则自动释放最近不使用的表。最大单个表大小是内存连接器限制的最大内存大小。表在创建后会在后台进程中自动持久化到磁盘。

### 逻辑分片（LogicalPart）

多个分片被进一步组织成逻辑分片。逻辑分片有一个可配置的最大容量（默认为 256 MB）。一旦最后一个可用的逻辑分片被填满，就会创建新的逻辑分片。作为表创建后的后台处理的一部分，数据在每一个逻辑分片中被排序和创建索引。基于下推的谓词，可以使用布隆过滤器和 MinMax 索引过滤掉不需要的逻辑分片。同时稀疏索引（Sparse Index）可以提供逻辑分片内部的更细致的过滤。

### 稀疏索引 （Sparse Index）

首先对页面进行排序，最后创建一个稀疏索引。稀疏索引不会记录所有的数据值而只会间隔抽取一些。这使得索引更小。稀疏索引有助于减少输入行，但不能执行完美的过滤。进一步的过滤会由 openLooKeng的Filter Operator完成。

参考图中的稀疏索引示例，这是内存连接器为不同查询过滤数据的方式：

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

- `DROP TABLE`之后，worker上的内存没有立即释放。内存在下一次对内存连接器进行创建操作后释放。
    - 可以通过创建一个临时的表来在worker上强制清理，如`CREATE TABLE memory.default.tmp AS SELECT * FROM tpch.tiny.nation;`
- 当前`sorted_by`只支持按一个列排序。
- 当前`memory.splits-per-node`必须在所有节点上都设为相同值。未来的优化将自动配置该项。