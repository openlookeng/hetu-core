# Release 1.7.0

## 关键特性

| 分类                  | 描述                                                         |
| --------------------- | ------------------------------------------------------------ |
| 算子处理扩展             | 增加通过用户自定义worker结点物理执行计划的生成，用户可以实现自己的算子pipeline代替原生实现，加速算子处理 |
| Task Recovery         | 1. 支持不开启快照特性时，实现失败任务重试<br/> 2. 支持在CLI端调试模式下显示快照相关统计信息：包括正在捕获的快照数量、已经捕获的快照数量、恢复次数（包括查询重启）、捕获的所有快照大小和捕获快照所需时间、用于恢复的快照大小和恢复时间<br/> 3. 资源管理：在任务消耗大量内存导致内存不足时自动暂停/恢复任务，用户也可以选择主动暂停和恢复任务（限于CTAS和INSERT INTO SELECT场景）<br/> 4. 增加可配置的失败重试策略：① 超时重试策略 ② 最大次数重试策略<br/> 5. 新增Gossip失败检测机制：控制节点通过定期接收心跳来监控工作节点是否存活，Gossip机制使得所有工作节点能够相互监测，确保所有节点所知的信息都是一致的，可以防止在集群中出现短暂网络故障时导致过早的查询失败<br/> |
| openLooKeng WEB UI增强 | 1. openLooKeng审计日志功能增强<br/>2. openLooKeng SQL历史记录持久化<br/> 3. SQL Editor支持自动联想<br/> 4. 支持常用SQL语句收藏<br/> 5. 数据源加载异常时，前端可通过异常标记显示<br/> 6. UI支持显示数据源信息|
| openLooKeng集群管理平台            | 1. 提供图形化的指标监控及定制，及时获取系统的关键信息<br/> 2. 提供服务属性的配置管理，满足实际业务的性能需求<br/> 3. 提供集群、服务、实例的操作功能，满足一键启停等操作需求<br/> 4. 提供openLooKeng在线部署功能，支持在线升级<br/> 5. 提供节点伸缩服务<br/> 6. 集成openLooKeng现有的Web UI<br/> 7. openLooKeng Manager工具支持用户登录/退出功能 |
## 已知问题

| 分类          | 描述                                                         | Gitee问题                                                 |
| ------------- | ------------------------------------------------------------ | --------------------------------------------------------- |
| Task Recovery | 启用快照时，执行带事务的CTAS语句时，中断一个worker服务，SQL语句执行报错          | [I5EAM3](https://e.gitee.com/open_lookeng/issues/list?issue=I5EAM3) |

## 获取文档

请参考： [https://gitee.com/openlookeng/hetu-core/tree/1.7.0/hetu-docs/zh](https://gitee.com/openlookeng/hetu-core/tree/1.7.0/hetu-docs/zh)