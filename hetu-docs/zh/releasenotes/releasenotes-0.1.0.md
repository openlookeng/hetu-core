
# Release 0.1.0

## 关键特性

|特性|描述|
| -------------------------------------------- | ------------------------------------------------------------ |
| Adaptive Dynamic Filter |动态特性增强，除了bloom filter外，还可以使用hashsets存储build侧值，用于过滤探针侧。筛选器存储在分布式内存存储中，以便后续查询可以重用它们，而不必重新构建。|
| Dynamically Add Catalog|添加目录REST api允许管理员在运行时添加任何连接器的新目录。目录属性文件被写入一个共享存储区，在那里它们被openLooKeng集群节点发现并注册和加载。使用该方法不需要重启集群。|
| Cross Region Dynamic Filter |跨数据中心场景，当探针侧在远端数据中心，构建侧在本地数据中心时，自适应过滤器会创建一个bloom filter。数据中心连接器增强，通过网络将bloom filter发送到远程数据中心，在远程数据中心中，它可用于筛选探针端。这减少了需要通过网络从远程数据中心传输到本地数据中心的数据量。|
| Horizontal Scaling Support|新增隔离节点和隔离节点状态，缩容时可静默节点。ISOLATED节点不接受任何新任务。|
|VDM  |虚拟数据集市，允许管理员创建虚拟目录和虚拟方案。在虚拟模式中，管理员可以在跨多个数据源的表上创建视图。虚拟数据集市简化了跨数据源和跨区域的表访问。|
| IUD support for ORC |支持Hive ORC表上的事务性插入、更新、删除。 仅支持Hive 版本3.x.|
| Compaction for ORC |支持hive ORC事务表的Compaction，通过增加每个读取器的数据获取来减少读取的文件数量，从而有助于提高查询性能，并提高并发度|
| CarbonData Connector with IUD support |支持CarbonData表的插入、更新、删除操作|
| Insert Overwrite |支持插入覆盖语法。这是裁剪和加载到现有表的简单方法|
| ODBC connector| ODBC PowerBI、Tableau、永洪桌面等第三方BI工具连接河图的驱动和网关|
|Dynamic Hive UDF|将自定义的Hive UDF动态加载到openLooKeng |
| HBase Connector| HBase连接器支持查询HBase数据源的数据|
| Enhance Create table statement |允许在openLooKeng中执行CREATE TABLE AS命令时指定所管理的hive表的外部位置;允许在openLooKeng中创建Hive事务表|
| Metadata Cache |引入通用元数据缓存SPI，为任何连接器提供透明的缓存层。如果缓存不存在，则元数据缓存委托连接器特定的元数据。目前JDBC连接器、DC连接器|
| Cross DC Connector |新的连接器被引入，以支持跨广域网的响应查询，允许客户端查询位于物理上遥远的另一个数据中心的数据源。|
| High Availability (Active-Active) |支持HA AA模式，将运行时状态信息存储在Hazelcast等分布式缓存中。黑兹尔卡斯特集群的形成是使用种子文件完成的。发现服务、OOM、CPU使用率使用分布式锁，以确保只有一个协调器启动这些服务。|
| Sql Migration Tool |协助Hive SQL迁移到openLooKeng兼容SQL的补充工具|
| 代码增强 |RawSlice Optimization:通过重用RawSlice对象，而不是构造新的Slice对象，减少openLooKeng的内存占用。Implicit Conversion ：支持数据类型隐式转换。例如：如果Insert语句中的查询类型与表类型不匹配，则可以将查询类型隐式转换为表类型。|

## 获取文档

请参考：[https://gitee.com/openlookeng/hetu-core/tree/010/hetu-docs/zh](https://gitee.com/openlookeng/hetu-core/tree/010/hetu-docs/zh)