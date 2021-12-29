# Release 1.5.0

## 关键特性

| 分类                  | 描述                                                         |
| --------------------- | ------------------------------------------------------------ |
| Star Tree             | 1. 支持优化连接查询，如星型模型的查询。<br/>2. 优化查询计划：由于Cube已经包含了汇总结果，所以改写查询计划将聚合运算结果重定向到Cube结果。当group by子句完全与Cube组匹配时性能收益明显。<br/>3. 问题修复，以进一步增强Cube的可用性和健壮性。 |
| Memory 连接器          | 1. 通过增加对内存表分区的支持，允许跳过整个分区的数据，从而提高内存连接器的性能。<br/>2. 收集内存表的统计信息，以支持基于openLooKeng代价的优化器。 |
| Task Recovery         | 修复了几个重要的错误，以解决数据不一致问题，以及在高并发和工作节点故障期间偶尔发生的查询挂起问题。 |
| Yarn上部署openLooKeng  | 支持在yarn上部署启用HA的openLoKeng集群实例，该实例包含一个反向代理（默认为ngnix）和2个或更多协调节点。通过增加和移除yarn容器，实现手动水平缩放openLooKeng集群。 |
| 数据持久化              | 优化了数据溢出到磁盘的机制，将序列化页面直接写入磁盘，而不是缓存。这样，算子可以释放更多内存，相比之前性能提高30%   |                                  |
## 已知问题

| 分类                  | 描述                                                         | Gitee问题                                                    |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Task Recovery         |在执行CTAS创建表stage1阶段，kill掉一个worker节点，偶现表数据减少   |[I4M2LW](https://e.gitee.com/open_lookeng/issues/list?issue=I4M2LW) |
| Memory 连接器          | 查询带index_columns参数创建的表时，报错：java.lang.NullPointerException | [I4NVW3](https://e.gitee.com/open_lookeng/issues/list?issue=I4NVW3)|
|                       | 删除分区列为double类型表后，重新创建相同表，count(*)查询结果行数增加。 | [I4LUDF](https://e.gitee.com/open_lookeng/issues/list?issue=I4LUDF) |

## 获取文档

请参考： [https://gitee.com/openlookeng/hetu-core/tree/1.5.0/hetu-docs/zh](https://gitee.com/openlookeng/hetu-core/tree/1.5.0/hetu-docs/zh)