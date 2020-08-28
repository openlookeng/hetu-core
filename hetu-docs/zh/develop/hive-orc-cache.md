# ORC Cache

ORC Cache功能可以通过缓存频繁访问的数据来提高查询性能。ORC Cache减少了TableScan操作所花费的时间，因为减少了网络IO所花费的时间。这又相应地降低了查询延迟。

在缓存访问频率最高且与openLooKeng部署不在同一位置的表中的原始数据时，该功能最有用。启用该功能后，工作节点会自动缓存所有ORC文件的文件尾(file tail)、条带页脚(stripe footer)、行索引、bloom索引，因为它们很小。不过，行组数据往往非常庞大，并且由于缓存大小的限制，为所有文件缓存行组数据实际上并不可行。

单击该[链接](https://orc.apache.org/specification/ORCv1/)以了解ORC规范。

可以使用`CACHE TABLE` SQL命令来配置工作节点缓存行数据的表和分区。

以下各节简要说明了整个行数据缓存实现的工作方式。

## SplitCacheMap

用户可以使用`CACHE TABLE` SQL语句来配置Hive连接器必须缓存的表和数据。要缓存的分区被定义为谓词，并存储在`SplitCacheMap`中。SplitCacheMap存储在协调节点的本地内存中。

以下是用于缓存2020年1月4日至2020年1月11期间每天的sales表数据的示例查询。

`cache table hive.default.sales where sales_date BETWEEN date '2020-01-04' AND date'2020-01-11'`

有关更多信息，请查看`CACHE TABLE`、`SHOW CACHE`和`DROP CACHE`命令。

SplitCacheMap存储以下两类信息：

1. 通过`CACHE TABLE`命令提供的表名和谓词。
2. 分段到工作节点的映射。

## 连接器

启用缓存并通过`CACHE TABLE` SQL命令提供谓词后，如果相应的分区ORC文件与谓词匹配，则连接器会将Hive分段标记为可缓存。

## SplitCacheAwareNodeSelector

通过实现SplitCacheAwareNodeSelector来支持Cache相关性调度。  SplitCacheAwareNodeSelector与负责将分段分配给工作节点的任何其他节点选择器相同。当第一次调度某个分段时，节点选择器会存储该分段以及为其调度了该分段的工作节点。对于后续调度，会使用该信息来确定是否已经有工作节点处理过该分段。如果已经有工作节点处理过该分段，则节点选择器将该分段调度到先前处理过该分段的工作节点上。如果没有工作节点处理过该分段，SplitCacheAwareNodeSelector将转而使用默认节点选择器来调度分段。  处理分段的工作节点会将分段映射的数据缓存到本地内存中。

## 工作节点

工作节点依赖`ConnectorSplit.isCacheable`方法来确定是否必须缓存分段数据。如果属性设置为true，则HiveConnector会尝试从缓存中检索数据。如果缓存未命中，则从HDFS中读取数据并将其存储在缓存中，以供将来使用。工作节点会根据到期时间和是否达到大小限制来清除其缓存，与协调节点无关。

请查看Hive连接器下的`ORC Cache Configuration`，以了解有关缓存配置的更多信息。