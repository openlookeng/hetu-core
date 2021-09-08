# Release 1.1.0 (2020年12月30日)

## 关键特性

| 分类       | 特性                                                         | PR #s                           |
| ---------- | ------------------------------------------------------------ | ------------------------------- |
| 启发式索引 | Btree索引-BTree索用于Split过滤，并且只能使用在Coordinator节点上，如果对作为查询谓词一部分的某一列创建索引，那么openLooKeng可以在执行的过程中通过索引来过滤掉Split，从而提高查询性能。 | 392,437,452,457                 |
|            | Bitmap索引-Bitmap索引用于读取ORC文件时过滤数据，并且只能使用在Worker节点上。Bitmap索引必须在高基数的列上创建。在这个发布版本中，对Bitmap索引进行了增强，将关键字存储在一个BTree中来加快关键字的查找，索引的创建以及减少索引的大小。 | 437,418,435,407,390             |
| 高性能     | 五个性能提升的特性：<br/>·     窗口函数的性能提升；<br/>·     将动态过滤等待时间修改为基于在Worker上的任务调度时间，而不包含花费在计划的时间；<br/>·     在Semi-join上使用动态过滤；<br/>·     实现Left-join转换成Semi-join的优化规则；<br/>·     实现Self-join转换成Group-by的优化规则。 | 429,366,385,406,391,330,446,382 |
|            | 很多的小Split会影响openLooKeng的查询性能，因为需要对这些Split进行调度，这样会导致更多的调度开销，完成的任务更少，并且每个Split的等待被读取的时间也会增加，为避免这种情况，使用合并小Split特性将小Split组合在一起，作为单个Split一起调度。 | 387                             |
|            | 交换复用特性引入了一个查询优化器，在一次查询中，一个表的数据被这条语句中的多个投影或过滤多次使用时，该特性会在内存中缓存该表数据来降低查询时间。 | 443                             |
| 连接器     | 新增支持openGauss 数据源                                     | 492                             |
|            | 新增支持MongoDB 数据源                                       | 464                             |
|            | 重构ElasticSearch 连接器以兼容7.x版本                        | 380                             |
|            | 新增Oracle连接器文档，并支持Oracle同义词特性。               | 380,487                         |
|            | 增强Hive 连接器以支持用户透传（impersonation）               | 454                             |
|            | ODBC语法兼容性能力增强                                       | 15,10,19,12,21,22               |
| DC 连接器  | 跨DC场景下动态过滤下推特性增强                               | 402,419                         |
| 用户体验   | 提供全新的WEB UI， 包含在线SQL编辑器和系统状态监控           | 163,368,404                     |
|            | 简化配置项                                                   | 397,369,449                     |
| 安全       | 支持与Apache Ranger 集成以实现统一的权限控制                 | 491                             |

## 已知问题

| 分类   | 描述                                                         | Gitee问题                                                    |
| ---------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| DC连接器   | 分层的数据中心模型中，分层大于3层，开启动态过滤的耗时可能会比关闭动态过滤的耗时更长，由于过滤的列不是高基数的，会导致过滤效果变差，所以需要引入更高效的过滤器。 | [I2BAZZ](https://gitee.com/openlookeng/hetu-core/issues/I2BAZZ) |
| 启发式索引 | 当删除index的时候，原HDFS上的文件夹并没有被删除，但里面的文件被删除。 | [I2BB1N](https://gitee.com/openlookeng/hetu-core/issues/I2BB1N) |
|            | 创建Index 并使用谓词‘or’ 时，显示的错误信息与实际功能不一致。这是因为缓存失效了，并且所有的Catalog、Schema、Table、Column信息被重新刷新导致。 | [I2BB3O](https://gitee.com/openlookeng/hetu-core/issues/I2BB3O) |
|            | 对事务分区表创建BTree索引后，不能过滤Split。                 | [I2BB6M](https://gitee.com/openlookeng/hetu-core/issues/I2BB6M) |
| WEB UI     | 点击刷新按钮后需要元数据树的相应时间很长。                   | [I2BB2B](https://gitee.com/openlookeng/hetu-core/issues/I2BB2B) |
|            | 如果有很多计划信息，文字会溢出文字区域。                     | [I2BB4E](https://gitee.com/openlookeng/hetu-core/issues/I2BB4E) |
| 安装        | 使用自动部署脚本，系统会使用旧的配置，没有任何提示，这样会误导用户。 | [I2BB52](https://gitee.com/openlookeng/hetu-core/issues/I2BB52) |
| Vacuum     | 如果对已执行删除/更新操作的表多次运行vacuum操作，则统计信息可能会被损坏，并导致查询将失败。为了避免这种情况，请在运行vaccum命令前将会话标志hive.collect_column_statistics_on_write设置为false (`set session hive.collect_column_statistics_on_write=false`)。如果在运行该命令时没有设置上述参数flag，执行ANALYZE <表名>命令进行统计信息修正。PR 517已修改，未合入1.1.0版本 | [I2BFH9](https://gitee.com/openlookeng/hetu-core/issues/I2BFH9) |
| Reuse Exchange        | 当在config.properties中启用Reuse Exchange特性（reuse_table_scan=true）时，查询非hive catalog会失败。建议在查询hive catalog时使用（设置session reuse_table_scan=true;），其他catalog时禁用。PR 516已修改，未合入1.1.0版本。 | [I2BEWV](https://gitee.com/openlookeng/hetu-core/issues/I2BEWV) |

## 获取文档

请参考：[https://gitee.com/openlookeng/hetu-core/tree/1.1.0/hetu-docs/zh](https://gitee.com/openlookeng/hetu-core/tree/1.1.0/hetu-docs/zh )