
基于成本的优化
========================

openLooKeng支持多种基于成本的优化，如下所述。

Join枚举
----------------

在查询中执行join的顺序会对查询的性能产生重大影响。join排序方面对性能影响最大的是网络处理和传输的数据的大小。如果一个产生大量数据的join在早期执行，那么后续的阶段需要处理大量数据的时间将长于需要的时间, 这样会增加查询所需的时间和资源。

使用基于成本的join枚举，openLooKeng使用连接器提供的`/optimizer/statistics`来评估不同join排序的成本，并自动选择计算成本最低的join排序。

join枚举策略由`join_reordering_strategy`会话属性控制，其中`optimizer.join-reordering-strategy`配置属性提供默认值。

有效值如下：

- `AUTOMATIC`（默认值） -启用全自动join枚举
- `ELIMINATE_CROSS_JOINS` -消除不必要的交叉join
- `NONE` -纯句法join顺序

如果使用`AUTOMATIC`但没有统计数据，或由于任何其他原因成本无法计算，则改用`ELIMINATE_CROSS_JOINS`策略。

Join分布选择
---------------------------

openLooKeng使用基于哈希的join算法。这意味着对于每个join操作符，必须从一个join输入（称为构建侧）创建哈希表。然后，迭代另一个输入（探针侧），并查询哈希表以找到匹配的行。

有两种类型的join分布：

- 分区模式：参与查询的每个节点仅从部分数据构建哈希表
- 广播模式：参与查询的每个节点从所有数据构建一个哈希表（数据复制到每个节点）

这两种类型各有利弊。分区join要求使用join键的散列重分布这两个表。这使得分区join比广播join慢（有时慢很多），但允许更大的join。特别是，当构建端比探测端小得多时，广播join将更快。但是，广播join要求join的构建端上的表在过滤后适合每个节点上的内存，而分布式join只需要适合所有节点上的分布式内存。

使用基于成本的join分布选择，openLooKeng自动选择使用分区join或广播join。使用基于成本的join枚举，openLooKeng自动选择哪一侧是探针侧，哪一侧是构建侧。

join分发策略由`join_distribution_type`会话属性控制，其中`join-distribution-type`配置属性提供默认值。

有效值如下：

- `AUTOMATIC`（默认值） -自动为每个join确定join分布类型
- `BROADCAST` -对所有join使用广播join分布
- `PARTITIONED` -对所有join使用分区join分布

动态过滤生成
-------------------------
根据构建测的`JoinNode`属性选择性的生成有效的动态过滤器。当其为高选择率时，仅生成主要的过滤器。其为低选择率时，则不生成过滤器。

使用基于成本的动态过滤生成，openLooKeng将根据构建测的`JoinNode`选择率高低来选择性的生成动态过滤。

动态过滤生成由`optimize_dynamic_filter_generation`会话属性控制启用。

有效值如下：
- `true` （默认值）- 启用动态过滤生成
- `false` - 不启用动态过滤生成

连接器实现
-------------------------

为了使openLooKeng优化器使用基于成本的策略，连接器实现必须提供`statistics`。
