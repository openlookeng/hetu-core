# Release 1.2.0

## 关键特性

| 分类             | 特性                                                         | PR #s                                         |
| ---------------- | ------------------------------------------------------------ | --------------------------------------------- |
| 启发式索引       | 针对启发式索引做了如下增强：<br/>1. 可以通过配置实现索引的预加载<br/>2. 当索引创建失败时，可以清理残留的索引记录。（之前版本，部分记录可能会残留，需要手动清理）<br/>3. 动态确定索引创建级别（之前版本，对于BtreeIndex用户必须指定level=table或level=partition，现在不再需要）<br/>4. 使用HetuMetastore进行索引元数据管理（删除基于文件的元数据管理）<br/>5. BTreeIndex：重构和支持AND、OR运算符<br/>6. BitmapIndex：支持其他运算符（>、>=、<、<=、IN、Between，请参见文档了解完整列表）<br/>7. 通过TestPrestoServer改进功能自动化测试<br/> | 646,627,604,<br/>537,609,601,<br/>529,611,600 |
| Star Tree索引    | 提供一种预聚合技术，可以优化冰山查询的延迟。<br/>1. 提供SQL 以管理Cubes（多维数据集）<br/>2. 支持创建分区Cubes<br/>3. 谓词支持为部分数据创建Cubes<br/>4. 针对构建大型Cubes, 支持增量插入 | 330                                           |
| 性能              | 针对CTE（具有with的语句）进行执行计划优化，使得CTE只被执行一次，并将其流式传输到查询中的其他引用，从而减少该查询所需的计算资源。减少计算资源有助于提高系统的总体并发。 | 623                                           |
|                  | 部分代码和工程化的优化，从而提高数据修改操作的性能           | 614,625,645                                   |
| Task Recovery    | 支持查询任务重试，当查询任务或者工作节点出现错误导致的查询失败时：<br/>1. 在查询执行期间捕获快照<br/>2. 检测到可恢复故障时，从最近成功的快照恢复查询执行<br/><br/>此功能目前还存在使用上的局限性。请参考相关文档 “管理-可靠查询执行”。当检测到不支持的场景时，会生成警告，并自动关闭该功能。<br/>目前该功能还是一个beta 版本，不建议在生产环境中使用。下方表格中列出了一些已知问题。 | 663                                           |
| 连接器           | HBase性能优化。针对单表查询性能的提升，我们新增了分片算法。另外，该优化支持 HBase 访问Snapshot的模式，从而提升多并发查询性能。 | 522,573                                       |
|                  | 支持外部函数（UDF）的注册，也支持将其下推到JDBC数据源。为了让大家拥有更好的体验，用户可以在不迁移自己数据源UDF的情况下，使用已有的UDF, 提高UDF的复用度，使用上更加方便。 | 647                                           |
|                  | 提供全新的算子下推框架，让算子下推过程更加简单灵活和高效。我们希望每个Connector可以参与到执行计划优化中。即引擎在进行执行计划优化时，能够自行应用Connector的优化规则。 | 633                                           |
| Web UI           | 1、元数据树修改为异步加载，优化定时全量加载导致的UI页面卡顿；<br/>2、优化显示问题，查询历史、查询结果分页显示；<br/>3、新增连接器的参数配置方式优化，无需自定义配置文件；<br/>4、增加节点状态显示； | 575,621,641,<br/>679                          |
| 安全             | 1. 增加行过滤、列掩码和认证用户模拟的权限控制接口，并集成到openLooKeng的Ranger权限管理插件实现对应权限管理；  <br/>2. 完善WEB UI登录用户的权限控制，登录用户只能获取本用户的查询记录<br/>3. 实现认证用户映射：定义了从认证系统中的用户到openLooKeng用户的映射规则；对于具有例如`alice/server1@example`等复杂用户名的Kerberos认证特别重要； | 539,586,667                                   |

## 已知问题

| 分类   | 描述                                                         | Gitee问题                                                    |
| ---------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 启发式索引    | 多CN场景下，存在show index行数不正确问题，但不影响index功能  | [I3E3T6](https://gitee.com/openlookeng/hetu-core/issues/I3E3T6) |
| WEB UI        | schema搜索栏只会在已展开的catalog下搜索schema名              | [I3E3Q3](https://gitee.com/openlookeng/hetu-core/issues/I3E3Q3) |
|               | 高并发场景，概率出现任务完成，UI一直显示进度100%             | [I3E3OK](https://gitee.com/openlookeng/hetu-core/issues/I3E3OK) |
| Task Recovery | Task Recovery过程中，恢复异常的worker节点，恢复任务会失败    | [I3CEWB](https://gitee.com/openlookeng/hetu-core/issues/I3CEWB) |
|               | CTAS创建桶表过程中，kill一个worker节点，恢复任务概率性失败   | [I3E3D8](https://gitee.com/openlookeng/hetu-core/issues/I3E3D8) |
|               | CTAS创建表过程中，kill一个worker节点，恢复任务概率性失败，报503错误 | [I3E3EH](https://gitee.com/openlookeng/hetu-core/issues/I3E3EH) |
|               | CTAS创建分区事务表过程中，kill一个worker节点，恢复任务概率性失败 | [I3E3F9](https://gitee.com/openlookeng/hetu-core/issues/I3E3F9) |
|               | 通过Task Recovery成功CTAS表，概率性出现插入行数翻倍问题      | [I3E3IG](https://gitee.com/openlookeng/hetu-core/issues/I3E3IG) |
|               | 通过Task Recovery成功CTAS表，概率性出现插入行数增加问题      | [I3E3JP](https://gitee.com/openlookeng/hetu-core/issues/I3E3JP) |
|               | 跨DC场景，CTAS创建表过程中，kill一个worker节点，恢复任务会失败 | [I3E3T6](https://gitee.com/openlookeng/hetu-core/issues/I3E3T6) |

## 获取文档

请参考：[https://gitee.com/openlookeng/hetu-core/tree/1.2.0/hetu-docs/zh](https://gitee.com/openlookeng/hetu-core/tree/1.2.0/hetu-docs/zh )
