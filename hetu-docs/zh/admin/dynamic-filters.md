# 动态过滤

本节介绍openLooKeng动态过滤特性。动态过滤适用于高选择性join场景，即大多数的probe侧的表在读取之后由于不匹配join条件而被过滤掉。
openLooKeng在查询运行时，依靠join条件以及build侧读出的数据，生成动态过滤条件，并作为额外的过滤条件应用到probe侧表的table scan阶段，从而减少参与join操作的probe表的数据量，有效地减少IO读取与网络传输。

## 适用场景
openLooKeng动态过滤当前适用于`inner join`以及`right join`场景，适用于`Hive connector`以及`DC connector`。

## 使用
openLooKeng动态过滤特性依赖于分布式缓存组件，请参考[Configuring HA](../installation/deployment-ha.md)章节配置`Hazelcast`。

在`/etc/config.properties‘需要配置如下参数

``` properties
enable-dynamic-filtering=true
dynamic-filtering-data-type=BLOOM_FILTER
dynamic-filtering-max-per-driver-size='100MB'
dynamic-filtering-max-per-driver-row-count=10000
dynamic-filtering-bloom-filter-fpp=0.1
```

上述属性说明如下：

- `enable-dynamic-filtering`：是否开启动态过滤特性。
- `dynamic-filtering-wait-time`：等待动态过滤条件生成的最长等待时间，默认值是0ms。（该配置要求集群不同节点之间的时间高度同步）
- `dynamic-filtering-data-type`：设置动态过滤类型，可选包含`BLOOM_FILTER`以及`HASHSET`，默认类型为`BLOOM_FILTER`。
- `dynamic-filtering-max-size`: 每个dynamic filter的大小上限，如果预估大小超过设定值，代价优化器不会生成对应的dynamic filter，默认值是1000000。
- `dynamic-filtering-max-per-driver-size`：每个driver可以收集的数据大小上限，默认值是1MB。
- `dynamic-filtering-max-per-driver-row-count`：每个driver可以收集的数据条目上限，默认值是10000。
- `dynamic-filtering-bloom-filter-fpp`：动态过滤使用的bloomfilter的FPP值，默认是0.1。

如果应用于`Hive connector`，需要对`catalog/hive.properties`如下修改：
``` properties
hive.dynamic-filter-partition-filtering=true
hive.dynamic-filtering-row-filtering-threshold=5000
```
上述属性说明如下：
- `hive.dynamic-filter-partition-filtering`：使用动态过滤条件根据分区值进行预先过滤，默认值是false。
- `hive.dynamic-filtering-row-filtering-threshold`：如果动态过滤条件大小低于阈值，则应用行过滤，默认值是2000。

