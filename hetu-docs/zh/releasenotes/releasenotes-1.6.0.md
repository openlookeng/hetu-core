# Release 1.6.0

## 关键特性

| 分类                  | 描述                                                         |
| --------------------- | ------------------------------------------------------------ |
| Star Tree             | 支持cube更新命令,允许管理员在基础数据更改时轻松更新现有cube的内容 |
| Bloom Index | 优化布隆过滤器索引大小使缩小十倍以上 |
| Task Recovery         | 1. 优化执行失败检测时间：当前需要300秒来确定任务失败,然后继续运行。改进这一点将改善执行流程和整体查询时间<br/> 2. 快照时间和大小优化：当执行过程中使用快照时,当前直接使用Java序列化,速度很慢,而且需要更多的空间。使用kryo序列化方式可以减小文件大小并提升速度来增加总吞吐量 |
| 数据持久化 | 1. 优化计算过程数据下盘速度和大小：当在Hash Aggregation（聚合算法）和GroupBy（分组）算子执行过程中发生溢出时,序列化到磁盘的数据会很慢,而且大小也会更大。因此可以通过减小大小和提高写入速度来提高整体性能。通过使用kryo序列化可以提高速度并减小溢出写盘文件大小<br/>2. 支持溢出到hdfs上：目前计算过程数据可以溢出到多个磁盘,现在支持溢出到hdfs以提高吞吐量<br/>3. 异步溢出/不溢出机制：当可操作内存超过阈值并触发溢出时,会阻塞接受来自下游运算符的数据。接受数据并加入到现有溢出流程将有助于更快地完成任务<br/>4. 支持右外连接&全连接场景下的溢出写盘：当连接类型为右外连接或全连接时,不会溢出构建侧数据,因为需要所有数据在内存中进行查找。当数据量较大时,这将导致内存溢出。因此，通过启用溢出机制并创建一个布隆过滤器来识别溢出的数据,并在与探查侧连接期间使用它 |
| 连接器增强            | 增强PostgreSQL和openGauss连接器，支持对数据源进行数据更新和删除操作 |
## 已知问题

| 分类          | 描述                                                         | Gitee问题                                                 |
| ------------- | ------------------------------------------------------------ | --------------------------------------------------------- |
| Task Recovery | 启用快照时，执行带事务的CTAS语句时，SQL语句执行报错          | [I502KF](https://e.gitee.com/open_lookeng/issues/list?issue=I502KF) |
|               | 启用快照并将exchange.is-timeout-failure-detection-enable关闭时，概率性出现错误 | [I4Y3TQ](https://e.gitee.com/open_lookeng/issues/list?issue=I4Y3TQ) |
| Star Tree     | 在内存连接器中，启用star tree功能后，查询时偶尔出现数据不一致 | [I4QQUB](https://e.gitee.com/open_lookeng/issues/list?issue=I4QQUB) |
|               | 当同时对10个不同的cube执行reload cube命令时，部分cube无法重新加载 | [I4VSVJ](https://e.gitee.com/open_lookeng/issues/list?issue=I4VSVJ) |

## 获取文档

请参考： [https://gitee.com/openlookeng/hetu-core/tree/1.6.0/hetu-docs/zh](https://gitee.com/openlookeng/hetu-core/tree/1.6.0/hetu-docs/zh)