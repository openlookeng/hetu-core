# Release 1.8.0

## 关键特性

| 分类     | 描述                                                                                                                        |
|--------|---------------------------------------------------------------------------------------------------------------------------|
| 任务级恢复  | 1. 通过使用物化的exchange信息仅重试失败的任务来实现查询恢复，支持Select场景的任务重试和恢复<br/> 2. 用于任务恢复的快照文件支持存放在HDFS上<br/> 3. 在实现物化exchange数据时，支持直接序列化和压缩。 |
| 查询资源管理 | 实现查询级别的资源监控，当任务超出资源组配置时，可支持任务的自动暂停/恢复、溢出可撤销内存、限制调度和终止查询。                                                                  |
| 连接器扩展  | singleData连接器为openGauss补充OLAP能力，增强数据分析竞争力。支持对接ShardingSphere和tidRange两种模式。                                                |
## 获取文档

请参考： [https://gitee.com/openlookeng/hetu-core/tree/1.8.0/hetu-docs/zh](https://gitee.com/openlookeng/hetu-core/tree/1.8.0/hetu-docs/zh)