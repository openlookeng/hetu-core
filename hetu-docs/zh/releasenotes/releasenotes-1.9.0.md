# Release 1.9.0

## 关键特性

| 分类     | 描述                                                                                                                        |
|--------|---------------------------------------------------------------------------------------------------------------------------|
| 任务级恢复  | 支持Create Table As Select场景的任务重试和恢复   |
| 连接器扩展  | 1. 新增Iceberg连接器，支持数据增删改查，分区演化，历史版本查询，事务，小文件合并<br/> 2. 增强Elastic Search连接器，支持算子下推，当前支持下推AND、OR、算术、比较运算符、BETWEEN、IN、GROUP BY、COUNT和SUM   |                                                                  |
| 性能增强    | 1. CTE优化，包括执行管道的重用，CTE结果的物化和重用<br/> 2. 执行计划优化，包括使用准确的分区，自适应聚合，join场景的选择率估计和使用能更好过滤的执行策略                                                  |
## 获取文档

请参考： [https://gitee.com/openlookeng/hetu-core/tree/1.9.0/hetu-docs/zh](https://gitee.com/openlookeng/hetu-core/tree/1.9.0/hetu-docs/zh)

