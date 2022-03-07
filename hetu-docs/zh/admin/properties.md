# 属性参考

本节将介绍最重要的配置属性，这些属性可用于调优openLooKeng或在需要时更改其行为。

## 通用属性

### `join-distribution-type`

> - **类型：** `string`
> - **允许值：** `AUTOMATIC`，`PARTITIONED`，`BROADCAST`
> - **默认值：** `AUTOMATIC`
> 
> 要使用的分布式联接的类型。  设置为`PARTITIONED`时，openLooKeng将使用哈希分布式联接。  当设置为`BROADCAST`时，将向集群中所有从左表获得数据的节点广播右表。分区联接要求使用联接键的哈希重分布这两个表。这可能比广播联接慢（有时极慢），但允许更大的联接。特别是如果右表比左表小得多，则广播联接将更快。  但是广播联接要求联接右侧过滤后的表适合每个节点的内存，而分布式联接只需要适合所有节点的分布式内存。当设置为`AUTOMATIC`时，openLooKeng将基于成本决定哪种分布类型是最优的。还将考虑将左右输入切换到联接。  在`AUTOMATIC`模式中，如果无法计算成本，例如表没有统计信息，openLooKeng将默认哈希分布式联接。也可以使用`join_distribution_type`会话属性在每个查询基础上指定。

### `redistribute-writes`

> - **类型：** `boolean`
> - **默认值：** `true`
> 
> 此属性允许在写入数据之前重新分布数据。这可以通过在集群中的节点间散列数据来消除数据倾斜带来的性能影响。当已知输出数据集没有发生倾斜时，可以停用数据分布，以避免在网络上散列和重分布所有数据的开销。也可以使用`redistribute_writes`会话属性在每个查询基础上指定。

### `stack-trace-visible`

> - **类型：** `boolean`
> - **允许值：** `true`, `false`
> - **默认值：** `false`
> 
> 此属性控制系统是否能够在CLI、WEB UI等对外展示系统出现Exception时的代码调用栈. 当设置为`true`时对外展示给所有用户，设置为`false`或者采用默认设置，不展示给任何用户。

### `openlookeng.admins`

> - **类型：** `string`
> - **默认值：** `不设置`
>
> 此属性用于设置admin用户，admin用户具有获取所有用户查询历史、下载所有用户WEB UI查询结果的权限。默认不设置admin用户，当需要设置多个admin用户时，多个用户间使用逗号隔开。

## http 安全头部属性

### `http-header.content-security-policy`

> - **类型：** `string`
> - **默认值：** `object-src 'none'`
> 
> 此属性设置 `content-security-policy` 设置相关值。

### `http-header.referrer-policy`

> - **类型：** `string`
> - **默认值：** `strict-origin-when-cross-origin`
> 
> 此属性设置 `referrer-policy` 设置相关值。

### `http-header.x-content-type-options`

> - **类型：** `string`
> - **默认值：** `nosniff`
> 
> 此属性设置 `content-security-policy` 设置相关值。

### `http-header.x-frame-options`

> - **类型：** `string`
> - **默认值：** `deny`
> 
> 此属性设置 `content-security-policy` 设置相关值。

### `http-header.x-permitted-cross-domain-policies`

> - **类型：** `string`
> - **默认值：** `master-only`
> 
> 此属性设置 `x-permitted-cross-domain-policies` 设置相关值。

### `http-header.x-xss-protection`

> - **类型：** `string`
> - **默认值：** `1; mode=block`
> 
> 此属性设置 `http-header.x-xss-protection` 设置相关值。

## 内存管理属性

### `query.max-memory-per-node`

> - **类型：** `data size`
> - **默认值：** `JVM max memory * 0.1`
> 
> 此属性是查询在工作节点上可以使用的最大用户内存量。用户内存是在执行期间为用户查询直接归属或可控制的事物分配的。例如，在执行期间构建的哈希表使用的内存、排序期间使用的内存等。当任何工作节点上的查询的用户内存分配达到此限制时，该工作节点将被杀死。

### `query.max-total-memory-per-node`

> - **类型：** `data size`
> - **默认值：** `JVM max memory * 0.3`
> 
> 此属性是查询在工作节点上可以使用的最大用户和系统内存量。系统内存是在执行期间为用户查询无法直接归属或可控制的事物分配的。例如，由读取器、写入器、网络缓冲区等分配的内存。当任何工作节点上的查询所分配的用户和系统内存的总和达到此限制时，该工作节点将被杀死。`query.max-total-memory-per-node`的值必须大于`query.max-memory-per-node`。

### `query.max-memory`

> - **类型：** `data size`
> - **默认值：** `20GB`
> 
> 此属性是查询在整个集群上可以使用的最大用户内存量。用户内存是在执行期间为用户查询直接归属或可控制的事物分配的。例如，在执行期间构建的哈希表使用的内存、排序期间使用的内存等。当一个跨所有工作节点的查询的用户内存分配达到此限制时，该工作节点将被杀死。

### `query.max-total-memory`

> - **类型：** `data size`
> - **默认值：** `query.max-memory * 2`
> 
> 此属性是查询在整个集群上可以使用的最大用户和系统内存量。系统内存是在执行期间为用户查询无法直接归属或可控制的事物分配的。例如，由读取器、写入器、网络缓冲区等分配的内存。当一个跨所有工作节点的查询所分配的用户和系统内存的总和达到此限制时，该工作节点将被杀死。`query.max-total-memory`的值必须大于`query.max-memory`。

### `memory.heap-headroom-per-node`

> - **类型：** `data size`
> - **默认值：** `JVM max memory * 0.3`
> 
> 此属性是在JVM堆中为openLooKeng不跟踪的分配留作裕量/缓冲区的内存量。

## 溢出属性

### `experimental.spill-enabled`

> - **类型：** `boolean`
> - **默认值：** `false`
> 
> 尝试将内存溢出到磁盘，以避免超出查询的内存限制。
> 
> 溢出是将内存卸载到磁盘。此过程允许内存占用大的查询，代价是执行时间变慢。聚合、联接（内联接和外联接）、排序和窗口函数支持溢出。此属性不会减少其他联接类型所需的内存使用。
> 
> 注意，这是一个实验特性，应谨慎使用。
> 
> 此配置属性可由`spill_enabled`会话属性重写。

### `experimental.spill-order-by`

> - **类型：** `boolean`
> - **默认值：** `true`
> 
> 尝试将内存溢出到磁盘，以避免在运行排序运算符时超出查询的内存限制。此属性必须与`experimental.spill-enabled`属性一起使用。
> 
> 此配置属性可由`spill_order_by`会话属性重写。

### `experimental.spill-window-operator`

> - **类型：** `boolean`
> - **默认值：** `true`
> 
> 尝试将内存溢出到磁盘，以避免在运行窗口运算符时超出查询的内存限制。此属性必须与`experimental.spill-enabled`属性一起使用。
> 
> 此配置属性可由`spill_window_operator`会话属性重写。

### `experimental.spill-build-for-outer-join-enabled`

> -   **类型：** `boolean`
> -   **默认值：** `false`
>
> 为右外连接和全外连接操作启用溢出功能。
>
> 此config属性可被`spill_build_for_outer_join_enabled`会话属性覆盖。

### `experimental.inner-join-spill-filter-enabled`

> -   **类型：** `boolean`
> -   **默认值：** `false`
>
> 启用基于布隆过滤器的构建侧溢出匹配，以进行探查侧溢出决策。
>
> 此config属性可被`inner_join_spill_filter_enabled`会话属性覆盖。

### `experimental.spill-reuse-tablescan`

> - **类型**：`boolean`
> - **默认值**：`false`
>
> 尝试将内存溢出到磁盘，以避免在运行Reuse Exchange时超出查询的内存限制。此属性必须与`experimental.spill-enabled`属性一起使用。
>
> 此配置属性可由`spill_reuse_tablescan`会话属性重写。

### `experimental.spiller-spill-path`

> - **类型：** `string`
> - **无默认值。** 启用溢出时必须设置。
> 
> 溢出内容写入的目录。该属性可以是一个逗号分隔的列表，以同时溢出到多个目录，这有助于利用系统中安装的多个驱动器。
> 当`experimental.spiller-spill-to-hdfs`为`true`时，`experimental.spiller-spill-path`必须只包含一个目录。
> 不建议溢出到系统驱动器上。最重要的是，不要溢出到写入JVM日志的驱动器，因为磁盘过度使用可能导致JVM长时间暂停，从而导致查询失败。

### `experimental.spiller-max-used-space-threshold`

> - **类型：** `double`
> - **默认值：** `0.9`
> 
> 如果指定溢出路径的磁盘空间使用率高于此阈值，则该溢出路径将不适用于溢出。

### `experimental.spiller-threads`

> - **类型：** `integer`
> - **默认值：** `4`
> 
> 溢出线程数。如果默认值不能使底层溢出设备饱和（例如，在使用RAID时），增大该值。

### `experimental.max-spill-per-node`

> - **类型：** `data size`
> - **默认值：** `100 GB`
> 
> 单个节点上所有查询使用的最大溢出空间。

### `experimental.query-max-spill-per-node`

> - **类型：** `data size`
> - **默认值：** `100 GB`
> 
> 单个查询在单个节点上使用的最大溢出空间。

### `experimental.aggregation-operator-unspill-memory-limit`

> - **类型：** `data size`
> - **默认值：** `4 MB`
> 
> 取消溢出单个聚合运算符实例所使用的内存限制。

### `experimental.spill-threshold-reuse-tablescan`

> - **类型**：`int`
> - **默认值**：`10（单位MB）`
>
> 用于在Reuse Exchange中缓存页面的内存限制。

### `experimental.spill-compression-enabled`

> - **类型：** `boolean`
> - **默认值：** `false`
> 
> 为溢出到磁盘的页面启用数据压缩。

### `experimental.spill-encryption-enabled`

> - **类型：** `boolean`
> - **默认值：** `false`
> 
> 允许使用随机生成的密钥（每个溢出文件）来加密和解密溢出到磁盘的数据。

### `experimental.spill-direct-serde-enabled`

> -   **类型：** `boolean`
> -   **默认值：** `false`
>
> 允许将页面直接序列化/读取到流中或从流中序列化/读取页面。

### `experimental.spill-prefetch-read-pages`

> -   **类型：** `integer`
> -   **默认值：** `1`
>
> 设置从溢出文件读取时预取的页数。


### `experimental.spill-use-kryo-serialization`

> -   **类型：** `boolean`
> -   **默认值：** `false`
>
> 启用基于Kryo的序列化以溢出到磁盘，而不使用默认的Java序列化器。


### `experimental.revocable-memory-selection-threshold`

> -   **类型：** `data size`
> -   **默认值：** `512 MB`
>
> 设置运算符可撤销内存的内存选择阈值，直接为准备撤销的剩余字节分配可撤销内存。

### `experimental.prioritize-larger-spilts-memory-revoke`

> -   **类型：** `boolean`
> -   **默认值：** `true`
>
> 启用对具有较大可撤销内存的Split进行优先级排序。

### `experimental.spill-non-blocking-orderby`

> -   **类型：** `boolean`
> -   **默认值：** `false`
>
> 开启按照运算符排序，使用异步机制溢出。即使在溢出正在进行时，也可以累积输入，并在次要数据累积超过阈值或主溢出完成时启动次溢出。阈值的默认值是20MB到可用内存的5%之间的最小值。此属性必须与`experimental.spill-enabled`属性结合使用。
>
> 此config属性可被`spill_non_blocking_orderby`会话属性覆盖。

### `experimental.spiller-spill-to-hdfs`

> -   **类型：** `boolean`
> -   **默认值：** `false`
>
> 启用溢出到HDFS。当此属性设置为`true`时，必须设置`experimental.spiller-spill-profile`属性，并且`experimental.spiller-spill-path`必须仅包含单个路径。

### `experimental.spiller-spill-profile`

> -   **类型：** `string`
> -   **无默认值。** 启用溢出到HDFS时必须设置此属性。
>
>
> 此属性定义用于溢出的[filesystem](../develop/filesystem.md)配置文件。对应的配置文件必须存在于`etc/filesystem`中。例如，如果此属性设置为`experimental.spiller-spill-profile=spill-hdfs`，则必须在`etc/filesystem`中创建描述此文件系统的配置文件`spill-hdfs.properties`，其中包含必要的信息，包括身份验证类型、config和keytab（如果适用，详情请参见[filesystem](../develop/filesystem.md）)。
>
> 当`experimental.spiller-spill-to-hdfs`设置为`true`时，必须配置此属性。所有Coordinator和Worker的配置文件中必须包含此属性。指定的文件系统必须可由所有Worker访问，并且Worker必须能够读取和写入指定文件系统中`experimental.spiller-spill-path`文件夹中指明的路径。

## 交换属性

在openLooKeng节点之间为查询的不同阶段交换数据。调整这些属性可有助于解决节点间通信问题或提高网络利用率。

### `exchange.client-threads`

> - **类型：** `integer`
> - **最小值：** `1`
> - **默认值：** `25`
> 
> 交换客户端从其他openLooKeng节点获取数据的线程数。对于大型集群或并发量非常高的集群，设置较高的值可以提高性能，但是过高的值可能会由于上下文切换和额外的内存使用而导致性能下降。

### `exchange.concurrent-request-multiplier`

> - **类型：** `integer`
> - **最小值：** `1`
> - **默认值：** `3`
> 
> 确定相对于可用缓冲区内存的并发请求数的乘数。根据每个请求的平均缓冲区使用量乘以该乘数，使用可适合可用缓冲区空间的客户端数量的启发式方法来确定最大请求数。例如，如果已经使用了`exchange.max-buffer-size`为`32 MB`和`20 MB`，每个请求的平均大小为`2MB`，则客户端的最大数量为`multiplier * ((32MB - 20MB) / 2MB) = multiplier * 6`。调整此值可以调整启发式，可能会增加并发度并提高网络利用率。

### `exchange.max-buffer-size`

> - **类型：** `data size`
> - **默认值：** `32MB`
> 
> 交换客户端中保存处理前从其他节点取出的数据的缓冲区大小。较大的缓冲区可以提高较大集群的网络吞吐量，从而减少查询处理时间，但会减少可用于其他用途的内存量。

### `exchange.max-response-size`

> - **类型：** `data size`
> - **最小值：** `1MB`
> - **默认值：** `16MB`
> 
> 交换请求返回的最大响应大小。响应将被放置在交换客户机缓冲区中，该缓冲区在交换的所有并发请求之间共享。
> 
> 如果网络延迟较高，增大该值可以提高网络吞吐量。减小该值可以提高大型集群的查询性能，因为它减少了由于交换客户端缓冲区保存了较多任务（而不是保存较少任务中的较多数据）的响应而导致的倾斜。

### `exchange.max-error-duration`

> - **类型：** `duration`
> - **最小值：** `1m`
> - **默认值：** `7m`
> 
> 交换错误最大缓冲时间，超过该时限则查询失败。

### `exchange.is-timeout-failure-detection-enabled`

> -   **类型：** `boolean`
> -   **默认值：** `true`
>
> 正在使用的故障检测机制。默认值是基于超时的故障检测。但是，当该属性设置为`false`时，启用基于最大重试次数的故障检测机制。

### `exchange.max-retry-count`

> -   **类型：** `integer`
> -   **默认值：** `10`
>
> Coordinator在将失败任务视为永久失败之前对其执行的最大重试次数。仅当`exchange.is-timeout-failure-detection-enabled`设置为`false`时，才使用此属性。

### `sink.max-buffer-size`

> - **类型：** `data size`
> - **默认值：** `32MB`
> 
> 上游任务等待拉取任务数据的输出缓冲区大小。如果任务输出是经过哈希分区的，那么缓冲区将在所有分区的使用者之间共享。如果网络延迟较高或集群中有多个节点，增加此值可以提高在阶段之间传输的数据的网络吞吐量。

## 任务属性

### `task.concurrency`

> - **类型：** `integer`
> - **限制：** 必须是2的幂。
> - **默认值：** `16`
> 
> 并行运算符（如联接和聚合）的默认本地并发度。该值应根据查询并发度和工作节点资源使用情况向上或向下调整。对于同时运行许多查询的集群来说，该值越低越好，因为所有正在运行的查询都已经利用了集群，所以增加更多的并发度将由于上下文切换和其他开销而导致速度变慢。对于一次只运行一个或较少查询的集群，该值越高越好。也可以使用`task_concurrency`会话属性在每个查询基础上指定。

### `task.http-response-threads`

> - **类型：** `integer`
> - **最小值：** `1`
> - **默认值：** `100`
> 
> 可以创建用于处理HTTP响应的最大线程数。线程是按需创建的，在空闲时被清理。因此，如果待处理的请求数量很少，则不会产生大量开销。在并发查询数高的集群上或在有数百或数千个工作节点的集群上，更多的线程可能会有帮助。

### `task.http-timeout-threads`

> - **类型：** `integer`
> - **最小值：** `1`
> - **默认值：** `3`
> 
> 生成HTTP响应时用于处理超时的线程数。如果所有线程都频繁使用，则应增大此值。这可以通过`io.prestosql.core.server:name=AsyncHttpExecutionMBean:TimeoutExecutor` JMX对象进行监视。如果`ActiveCount`始终与`PoolSize`相同，则增加线程数。

### `task.info-update-interval`

> - **类型：** `duration`
> - **最小值：** `1ms`
> - **最大值：** `10s`
> - **默认值：** `3s`
> 
> 控制任务信息的时效性，用于调度。较大的值可以降低协调节点CPU负载，但可能导致次优的分片调度。

### `task.max-partial-aggregation-memory`

> - **类型：** `data size`
> - **默认值：** `16MB`
> 
> 分布式聚合时部分聚合结果的最大大小。增大此值可以允许在刷新之前在本地保留更多的组，从而减少网络传输和CPU利用率，但要以增加内存利用率为代价。

### `task.max-worker-threads`

> - **类型：** `integer`
> - **默认值：** `Node CPUs * 2`
> 
> 设置工作节点用来处理分片的线程数。如果工作节点CPU利用率较低且所有线程都在使用，则增加此数量可以提高吞吐量，但会导致堆空间使用率增加。设置过高的值可能会由于上下文切换而导致性能下降。通过`io.prestosql.core.execution.executor:name=TaskExecutor.RunningSplits` JXM对象的`RunningSplits`属性可以获得活动线程的数量。

### `task.min-drivers`

> - **类型：** `integer`
> - **默认值：** `task.max-worker-threads * 2`
> 
> 工作节点上运行中的叶子分片的目标个数。这是一个最小值，因为每个叶任务保证至少`3`个运行分片。还保证运行非叶子任务，以防止死锁。较低的值可能提高对新任务的响应能力，但可能导致资源利用不足。较高的值可以提高资源利用率，但会占用额外的内存。

### `task.writer-count`

> - **类型：** `integer`
> - **限制：** 必须是2的幂。
> - **默认值：** `1`
> 
> 每个工作节点每个查询的并发写入器线程数。增加该值可以提高写入速度，尤其在查询不是I/O绑定并且可以利用额外的CPU进行并行写入时。（某些连接器由于压缩或其他原因，在写入时可能会在CPU上出现瓶颈）.设置该值过高可能导致集群因资源使用率过高而过载。也可以使用`task_writer_count`会话属性在每个查询基础上指定。

## 节点调度器属性

### `node-scheduler.max-splits-per-node`

> - **类型：** `integer`
> - **默认值：** `100`
> 
> 每个工作节点可以运行的分片总数的目标值。
> 
> 如果要批量提交查询（例如，定期运行大量报告），或者对于产生许多分片且快速完成的连接器，建议使用较高的值。增加此值可以确保工作节点有足够的分片来充分利用，从而改善查询延迟。
> 
> 设置此值过高将浪费内存，并可能导致性能降低，因为分片在工作节点之间不平衡。理想情况下，应该设置始终至少有一个分片等待处理，但不要更高。

### `node-scheduler.max-pending-splits-per-task`

> - **类型：** `integer`
> - **默认值：** `10`
> 
> 每个工作节点在单个查询阶段可以排队等待的未处理分片数，即使该节点已经处于总分片数的限制。每个阶段需要允许最小数量的分片以防止饥饿和死锁。
> 
> 此值必须小于`node-scheduler.max-splits-per-node`，通常由于相同的原因而增加。如果设置过高，也有类似的缺点。

### `node-scheduler.min-candidates`

> - **类型：** `integer`
> - **最小值：** `1`
> - **默认值：** `10`
> 
> 选择分片的目标节点时节点调度器将评估的最小候选节点数。将此值设置过低可能会使无法在所有工作节点之间适当平衡。将此值设置过高可能会增加查询延迟，并增加协调器CPU使用率。

### `node-scheduler.network-topology`

> - **类型：** `string`
> - **允许值：** `legacy`，`flat`
> - **默认值：** `legacy`
> 
> 设置调度分片时使用的网络拓扑。`legacy`调度分片时忽略拓扑。`flat`会尝试在数据所在的主机上调度分片，为本地分片预留50%的工作队列。对于分布式存储与openLooKeng worker运行在相同节点上的集群，推荐使用`flat`。

## 优化器属性

### `optimizer.dictionary-aggregation`

> - **类型：** `boolean`
> - **默认值：** `false`
> 
> 对字典上的聚合启用优化。也可以使用`dictionary_aggregation`会话属性在每个查询基础上指定。

### `optimizer.optimize-hash-generation`

> - **类型：** `boolean`
> - **默认值：** `true`
> 
> 在执行期间的早期为分布、联接和聚合计算哈希代码，允许在查询的后期在操作之间共享结果。这可以通过避免多次计算相同的哈希来降低CPU使用率，但代价是为哈希进行额外的网络传输。在大多数情况下，这将减少整个查询处理时间。也可以使用`optimize_hash_generation`会话属性在每个查询基础上指定。
> 
> 在使用[EXPLAIN](../sql/explain.md)时禁用此属性通常很有帮助，这样可以使查询计划更易读。

### `optimizer.optimize-metadata-queries`

> - **类型：** `boolean`
> - **默认值：** `false`
> 
> 通过使用存储为元数据的值来启用对一些聚合的优化。这允许openLooKeng在恒定的时间内执行一些简单的查询。目前，该优化适用于分区键的`max`、`min`和`approx_distinct`，以及其它对输入（包括`DISTINCT`聚集）的基数不敏感的聚集。使用此属性可以大大加快某些查询的速度。
> 
> 主要的缺点是，如果连接器为没有行的分区返回分区键，可能会产生不正确的结果。特别是，如果空分区是由其他系统创建的（openLooKeng不能创建），那么Hive连接器可以返回空分区。

### `optimizer.push-aggregation-through-join`

> - **类型：** `boolean`
> - **默认值：** `true`
> 
> 如果聚合位于外联接上，并且来自联接外部的所有列都在分组子句中，则聚合被推送到外联接之下。这种优化对于相关的标量子查询尤其有用，这些子查询通过外联接被重写为聚合。例如：
> 
> ```sql
> SELECT * FROM item i
>        WHERE i.i_current_price > (
>            SELECT AVG(j.i_current_price) FROM item j
>                WHERE i.i_category = j.i_category);
> ```
> 
> 启用此优化可以减少联接需要处理的数据量，从而大大加快查询速度。  但是，此优化可能会减慢一些具有非常选择性联接的查询。也可以使用`push_aggregation_through_join`会话属性在每个查询基础上指定。

### `optimizer.push-table-write-through-union`

> - **类型：** `boolean`
> - **默认值：** `true`
> 
> 在写入数据的查询中使用`UNION ALL`时，对写入进行并行化。这提高了在`UNION ALL`查询中写入输出表的速度，因为这些写入在收集结果时不需要额外的同步。当写入速度尚未饱和时，启用此优化可以提高`UNION ALL`速度。但是，此优化可能会减慢负载已经很重的系统中的查询。也可以使用`push_table_write_through_union`会话属性在每个查询基础上指定。

### `optimizer.join-reordering-strategy`

> - **类型：** `string`
> - **允许值：** `AUTOMATIC`，`ELIMINATE_CROSS_JOINS`，`NONE`
> - **默认值：** `AUTOMATIC`
> 
> 要使用的联接重新排序策略。  `NONE`维持查询中列出的表的顺序。  `ELIMINATE_CROSS_JOINS`重新排序联接，以尽可能消除交叉联接，否则保持原始查询顺序。当重新排序连接时，该值还尽可能地保持原来的表顺序。`AUTOMATIC`枚举可能的顺序并使用基于统计的成本估计来确定最小成本顺序。如果统计数据不可用，或者由于任何原因无法计算成本，则使用`ELIMINATE_CROSS_JOINS`策略。也可以使用`join_reordering_strategy`会话属性在每个查询基础上指定。

### `optimizer.max-reordered-joins`

> - **类型：** `integer`
> - **默认值：** `9`
> 
> 当optimizer.join-reordering-strategy设置为基于成本时，此属性确定可一次重新排序的最大联接数。
> 
> **警告**
> 
> 可能的连接顺序数随着关系数的增大而增大，因此增加此值会导致严重的性能问题。

### `hetu.query-pushdown`

> - **类型：** `boolean`
> - **默认值：** `true`
>
> 控制jdbc connector及dc connector下推的总开关。

### `optimizer.reuse-table-scan`

> - **类型**：`boolean`
> - **默认值**：`false`
>
> 如果查询包含的表或公用表表达式（CTE）出现多次且具有相同的投影和过滤器，则使用Reuse Exchange来将数据缓存在内存中。启用此功能将通过将数据缓存在内存中并避免多次从磁盘读取来减少执行查询所需的时间。也可以使用`reuse_table_scan`会话属性在每个查询基础上指定。
>
> 注意：当启用`cte_reuse_enabled`或`optimizer.cte-reuse-enabled`时，重用交换将被禁用。

### `optimizer.cte-reuse-enabled`

> - **类型：** `boolean`
> - **默认值：** `false`
>
> 启用此标志后，无论主查询中使用同一CTE多少次，都仅执行一次公用表表达式（CTE）。当多次使用同一个CTE时，这将有助于提高查询执行性能。也可以使用 cte_reuse_enabled 会话属性对每个查询指定。

### `optimizer.sort-based-aggregation-enabled`

> -   **类型：** `boolean`
> -   **默认值：** `false`
>
> 当基础源处于预排序顺序时，使用基于排序的聚合，而不是哈希聚合，后者需要占用更多的空间来构建哈希表。
> 与哈希聚合相比，基于排序的聚合占用的内存空间较少。
> Hive中基于排序聚合的条件
> - 1) 分组列数量应等于或小于排序列，且顺序应与排序列相同。
> - 2) Join探针侧表应排序，Join条件数量应等于或小于已排序列，且顺序应与排序列相同。
> - 3) 当bucket_count为1时，bucketed_by列数量应等于或小于分组列，且顺序应与分组列相同。
> - 4) 当bucket_count大于1时，bucketed_by列数量和顺序应与分组列相同。
> - 5) 针对分区表，分组列应包含所有分区，顺序与按列排序的子集顺序一致。
> - 6) 使用 distinct 时，带有distinct列的分组列应该是排序列集合的子集。
>
> 也可以使用`sort_based_aggregation_enabled`会话属性在每个查询上指定。
>
> **注意：** 仅适用于Hive连接器。

## 正则表达式函数属性

下列属性允许调优[正则表达式函数](../functions/regexp.md)。

### `regex-library`

> - **类型：** `string`
> - **允许值：** `JONI`，`RE2J`
> - **默认值：** `JONI`
> 
> 用于正则表达式函数的库。一般来说，`JONI`对于一般用途的速度要快一些，但是对于某些表达式模式可能需要指数级的时间。`RE2J`使用不同的算法保证线性时间，但通常速度较慢。

### `re2j.dfa-states-limit`

> - **类型：** `integer`
> - **最小值：** `2`
> - **默认值：** `2147483647`
> 
> RE2J在为正则表达式匹配构建快速但可能占用大量内存的确定性有限自动机（DFA）时所使用的最大状态数。如果达到限制，RE2J将回落到使用速度较慢但较少内存密集型非确定性有限自动机（NFA）的算法。减小此值会降低正则表达式搜索的最大内存占用，但会牺牲速度。

### `re2j.dfa-retries`

> - **类型：** `integer`
> - **最小值：** `0`
> - **默认值：** `5`
> 
> 在RE2J使用较慢但较少内存密集型的NFA算法对所有后续输入进行搜索前，如果DFA算法达到状态限制，RE2J将重试该算法的次数。如果遇到给定输入行的极限值可能是离群值，那么你希望能够使用更快的DFA算法来处理后续行。如果你也有可能达到匹配后续行的限制，那么你应该从头开始使用正确的算法，以避免浪费时间和资源。处理的行数越多，该值应该越大。

## 启发式索引属性

启发式索引是外部索引模块，可用于过滤连接器级别的行。 位图，Bloom和MinMaxIndex是openLooKeng提供的索引列表。 到目前为止，位图索引支持使用ORC存储格式的表支持蜂巢连接器。

### `hetu.heuristicindex.filter.enabled`

> - **类型：** `boolean`
> - **默认值：** `false`
> 
> 此属性启用启发式索引。还有一个会话属性`heuristicindex_filter_enabled`，可按会话设置。注意：当配置文件中将此全局属性设置为`true`时，会话属性仅用于临时打开和关闭索引筛选。当未全局启用索引筛选器时，无法使用该会话属性来打开。

### `hetu.heuristicindex.filter.cache.max-memory`

> -   **类型：** `data size`
> -   **默认值：** `10GB`
>
> 由于索引文件很少被改动，将索引缓存可以提升性能，减少从文件系统读取索引所需时间。这一属性控制索引缓存允许使用的内存大小，当缓存已满，最旧的缓存将被移除，由新的缓存替代（LRU缓存）。

### `hetu.heuristicindex.filter.cache.soft-reference`

> -   **类型:** `boolean`
> -   **默认值：** `true`
>
> 缓存索引可以提供更好的性能，但是需要使用一部分内存空间。启用这一属性将允许垃圾回收器（GC）在内存不足时从缓存中清除内容来释放内存。
>
> 注意：这一特性还在实验中，请谨慎使用！

### `hetu.heuristicindex.filter.cache.ttl`

> - 类型：`Duration`
> - **默认值：** `24h`
> 
> 索引缓存的有效时间。

### `hetu.heuristicindex.filter.cache.loading-threads`

> - 类型：`integer`
> - **默认值：** `10`
> 
> 从索引存储文件系统并行加载索引时使用的线程数量。

### `hetu.heuristicindex.filter.cache.loading-delay`

> - 类型：`Duration`
> - **默认值：** `10s`
> 
> 在异步加载索引到缓存前等待的时长。

### `hetu.heuristicindex.filter.cache.preload-indices`

> - 类型：`string`
> - **默认值：** ``
>
> 在服务器启动时预加载指定名称的索引(用逗号分隔), 当值为`ALL`时将预载入全部索引。

### `hetu.heuristicindex.indexstore.uri`

> - 类型：`string`
> - **默认值：** `/opt/hetu/indices/`
> 
> 所有索引文件存储在的目录。 每个索引将存储在其自己的子目录中。

### `hetu.heuristicindex.indexstore.filesystem.profile`

> - 类型 `string`
>
> 此属性定义用于读取和写入索引的文件系统配置文件。对应的配置文件必须存在于`etc/filesystem`中。例如，如果将该属性设置为`hetu.heuristicindex.filter.indexstore.filesystem.profile=index-hdfs1`，则必须在`etc/filesystem`中创建描述该文件系统访问的配置文件`index-hdfs1.properties`，其中包含的必要信息包括身份验证类型、配置和密钥表（如适用）。
>
> `LOCAL`文件系统类型仅应在测试期间或单节点群集中使用。
>
> 应在生产中使用`HDFS`文件系统类型，以便集群中的所有节点都能访问索引。所有节点都应配置为使用相同的文件系统配置文件。

## 执行计划缓存属性

执行计划缓存功能允许协调器在相同的查询之间重用执行计划， 构建另一个执行计划的过程，从而减少了所需的查询预处理量。

### `hetu.executionplan.cache.enabled`

> - **类型：** `boolean`
> - **默认值：** `false`
> 
> 启用或禁用执行计划缓存。 默认禁用。

### `hetu.executionplan.cache.limit`

> - **类型：** `integer`
> - **默认值：** `10000`
> 
> 保留在缓存中的最大执行计划数

### `hetu.executionplan.cache.timeout`

> - **类型：** `integer`
> - **默认值：** `86400000 ms`
> 
> 上次访问后使缓存的执行计划失效的时间（以毫秒为单位）

## SplitCacheMap属性

必须启用SplitCacheMap以支持缓存行数据。 启用后，协调器将存储表，分区和分片调度元数据 帮助进行缓存亲和力调度。

### `hetu.split-cache-map.enabled`

> - **类型：** `boolean`
> - **默认值：** `false`
> 
> 此属性启用分片缓存功能。 如果启用了状态存储，则分片缓存映射配置也会自动复制到状态存储中。 在具有多个协调器的HA设置的情况下，状态存储用于在协调器之间共享分片的缓存映射。

### `hetu.split-cache-map.state-update-interval`

> - **类型：** `integer`
> - **默认值：** `2 seconds`
> 
> 此属性控制在状态存储中更新分割缓存映射的频率。 它主要适用于HA部署。

## 自动清空

> 自动清空使系统能够通过持续监测需要清空的表来自动管理清空作业，以保持最佳性能。引擎从符合清空条件的数据源获取表，并触发对这些表的清空操作。

### `auto-vacuum.enabled`

> - **类型：** `boolean`
> - **默认值：** `false`
> 
> 此属性用于启用自动清空功能。
>
> **注意：** 此属性只能在协调节点中配置。

### `auto-vacuum.scan.interval`

> - **类型：** `Duration`
> - **默认值：** `10m`
> 
> 此属性为从数据源获取情况表信息并触发对这些表的清空操作的定时间隔。计时器在服务器启动时开始，并将在配置的间隔内保持调度。最小值为15s，最大值为24h。
>
> **注意：** 此属性只能在协调节点中配置。

### `auto-vacuum.scan.threads`

> - **类型：** `integer`
> - **默认值：** `3`
> 
> 用于自动清空功能的线程数。最小值为1，最大值为16。
>
> **注意：** 此属性只能在协调节点中配置。

## CTE属性

### `cte.cte-max-queue-size`

> - **类型：** `int`
> - **默认值：** `1024`
>
> 每个处理队列的最大页数。处理队列的数量等于主查询中的CTE引用的数量。也可以使用 cte_max_queue_size 会话属性对每个查询指定。

### `cte.cte-max-prefetch-queue-size`

> - **类型：** `int`
> - **默认值：** `512`
>
> 处理队列已满时，预取队列可以容纳的最大页数。预取队列用于急切读取数据，从而无需等待I/O执行查询。也可以使用 cte_max_prefetch_queue_size会话属性对每个查询指定。
>
> **说明：** 应在所有工作节点上配置该属性。

## 排序基础聚合属性

### `sort.prcnt-drivers-for-partial-aggr`

> -   **类型：** `int`
> -   **默认值：** `5`
>
> 在基于排序的聚合中，用于未完成/部分值的驱动程序数的百分比。
> 也可以使用`prcnt_drivers_for_partial_aggr`会话属性在每个查询上指定。
>
> **注意：** 应在所有节点上配置该属性。

## 查询管理

### `query.remote-task.max-error-duration`

> - 类型：`duration`
> - **默认值**：`5m`
> 
> 远程任务错误最大缓冲时间，超过该时限则查询失败。

## 分布式快照

### `snapshot_enabled`

> - 类型：`boolean`
> - **默认值**：`false`
>
> 此会话属性用于启用或禁用分布式快照功能。

### `hetu.experimental.snapshot.profile`

> - 类型：`string`
>
> 此属性定义用于存储快照的[文件系统](../develop/filesystem.md)配置文件。对应的配置文件必须存在于`etc/filesystem`中。例如，如果将该属性设置为`hetu.experimental.snapshot.profile=snapshot-hdfs1`，则必须在`etc/filesystem`中创建描述此文件系统的配置文件`snapshot-hdfs1.properties`，其中包含的必要信息包括身份验证类型、配置和密钥表（如适用）。具体细节请参考[文件系统](../develop/filesystem.md)相关章节。
>
> 如在打开分布式快照的情况下执行任何查询时，需要配本属性。此属性必须包含在所有协调节点和工作节点的配置文件中。指定的文件系统必须可由所有工作节点访问，且这些工作节点必须能够读取和写入指定文件系统中的`/tmp/hetu/snapshot`文件夹。
>
> 作为实验性属性，或可以将快照存储在非文件系统位置，如连接器。

### `hetu.snapshot.maxRetries`

> - 类型：`int`
> - **默认值**：`10`
>
> 此属性定义查询的错误恢复尝试的最大次数。达到限制时，查询失败。
>
> 也可以使用`snapshot_max_retries`会话属性在每个查询基础上指定。

### `hetu.snapshot.retryTimeout`

> - **类型：** `duration`
> - **默认值：** `10m`（10分钟）
>
> 此属性定义系统等待所有任务成功恢复的最大时长。如果在此超时时限内任何任务未就绪，则认为恢复失败，查询将尝试从较早快照恢复（如果可用）。
>
> 也可以使用`snapshot_retry_timeout`会话属性在每个查询基础上指定。

### `hetu.snapshot.useKryoSerialization`

> -   **类型：** `boolean`
> -   **默认值：** `false`
>
> 为快照启用基于Kryo的序列化，而不是默认的Java序列化。

## HTTP客户端属性配置

### `http.client.idle-timeout`

> -   **类型:** `duration`
> -   **默认值:** `30s` （30秒）
>
> 此参数定义了当http客户端没有任何操作时，其保持连接的时间。
> 当超过指定时间还没有任何操作的话，将关闭客户端并释放相关资源。
>
> （注意：建议在高负载环境下，该参数配置大一点。）

### `http.client.request-timeout`

> -   **类型:** `duration`
> -   **默认值:** `10s` （10秒）
>
> 此参数定义了http客户端接收响应的时间阈值。
> 当超过所配置时间，客户端没有接收到任何响应，则视为客户端的请求提交失败。
>
> （注意： 建议在高负载环境下，该参数配置大一点。）