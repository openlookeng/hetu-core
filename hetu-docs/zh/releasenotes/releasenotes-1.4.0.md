# Release 1.4.0

## 关键特性

| 分类                  | 描述                                                         |
| --------------------- | ------------------------------------------------------------ |
| Star Tree             | 1. 更新APIs支持jdbc 连接器使用cube<br/>2. 支持Clickhouse连接器使用cube，其它jdbc connector在将来支持<br/>3. 问题修复 |
| 启发式索引            | 1. 支持UPDATE INDEX（详细见文档）<br/>2. SHOW INDEX中增加index大小，内存和磁盘占用信息<br/>3. Bloom index增加nmap cache来减少内存使用（默认打开，详细见文档）<br/>4. 支持DROP TABLE同时删除index<br/>5. 支持创建index后自动加载index（默认打开，详细见文档） |
| Memory 连接器         | 修复了几个重要的错误，以解决大型数据集和特定运算符偶尔发生的错误结果 |
| Task Recovery         | 修复了几个重要的错误，以解决高并发和worker节点故障期间偶尔发生的数据不一致问题 |
| Yarn上部署openLooKeng | 支持Yarn上部署openLooKeng集群。当前支持部署单coordinator和多worker节点部署 |
| 低时延                | 1. 优化不包含join的点查sql的stats计算，加快低时延查询的速度<br/>2. 新增自适应分片分组，提升高并发查询吞吐量<br/>3. 支持非等式动态筛选器，以加快具有<、>、<= & >=等谓词的查询速度 |
| Kylin 连接器          | 支持对kylin数据源的读操作                                    |
## 已知问题

| 分类                  | 描述                                                         | Gitee问题                                                    |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Task Recovery         | 并发CTAS，部分sql报错：“HiveSplitSource is already closed”   | [144QYL](https://e.gitee.com/open_lookeng/issues/list?issue=I44QYL) |
|                       | CTA创建事务表，插入数据报错“Unsuccessful query retry”        | [I44RQJ](https://e.gitee.com/open_lookeng/issues/list?issue=I44RQJ) |
| Yarn上部署openLooKeng | 启动多个openLooKeng小集群，containers会在相同的节点上启动，导致此节点资源会用完，新启openLooKeng集群失败（实际其它节点还有资源） | [I4BP5A](https://e.gitee.com/open_lookeng/issues/list?issue=I4BP5A) |
|                       | 启动多个openLooKeng集群，部分集群进CLI报错                   | [I4BY6Z](https://e.gitee.com/open_lookeng/issues/list?issue=I4BY6Z) |

## 获取文档

请参考： [https://gitee.com/openlookeng/hetu-core/tree/1.4.0/hetu-docs/zh](https://gitee.com/openlookeng/hetu-core/tree/1.4.0/hetu-docs/zh)