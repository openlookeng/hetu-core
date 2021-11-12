# OmniData连接器

## 概述

OmniData连接器支持查询存储在远端Hive数据仓库中的数据。它通过将openLooKeng的算子下推到存储节点以实现近数据计算，从而降低网络传输数据量、提升计算性能。

更多信息请见：[OmniData](https://www.hikunpeng.com/zh/developer/boostkit/big-data?accelerated=3)和[OmniData connector](https://github.com/kunpengcompute/omnidata-openlookeng-connector)。

## 支持的文件类型

OmniData连接器支持以下文件类型：

- ORC
- Parquet
- Text

## 配置

用以下内容创建`etc/catalog/omnidata.properties`，并将`example.net:9083`替换为Hive元存储Thrift服务的主机和端口：

```properties
connector.name=omnidata-openlookeng
hive.metastore.uri=thrift://example.net:9083
```

### HDFS配置

通常，openLooKeng会自动配置HDFS客户端，不需要任何配置文件。在某些情况下，例如使用联邦HDFS或NameNode高可用性时，需要指定额外的HDFS客户端选项，以便访问HDFS集群。可通过添加`hive.config.resources`属性来引用HDFS配置文件：

```properties
hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
```

所有openLooKeng节点上都必须存在这些配置文件。如果你正在引用现有的Hadoop配置文件，请确保将其复制到所有没有运行Hadoop的openLooKeng节点。

## OmniData配置属性

| 属性名称                        | 说明                                                         | 默认值 |
| ------------------------------- | ------------------------------------------------------------ | ------ |
| hive.metastore                  | Hive元存储类型                                               | thrift |
| hive.config.resources           | 以逗号分隔的可选HDFS配置文件列表。这些文件必须存在于运行openLooKeng的机器上。该属性仅在访问HDFS绝对必要的情况下指定。示例：`/etc/hdfs-site.xml` |        |
| hive.omnidata-enabled           | 允许下推算子到存储侧执行。如果被禁用，所有算子将不会被下推。 | true   |
| hive.min-offload-row-number     | 若数据表的行数小于该阈值，则该数据表的所有算子将不会被下推。 | 500    |
| hive.filter-offload-enabled     | 允许下推filter算子到存储侧执行。如果被禁用，filter算子将不会被下推。 | true   |
| hive.filter-offload-factor      | filter算子被下推时的数据选择率阈值。只有当算子的选择率小于该阈值，该算子它才会被下推。 | 0.25   |
| hive.aggregator-offload-enabled | 允许下推aggregator算子到存储侧执行。如果被禁用，aggregator算子将不会被下推。 | true   |
| hive.aggregator-offload-factor  | aggregator算子被下推时的数据聚合率阈值。只有当算子的聚合率小于该阈值，该算子才会被下推。 | 0.25   |

更多的配置，请参阅[Hive配置属性](./hive.md#Hive配置属性)章节。

### 查询OmniData

SQL的算子被下推后的执行计划：

```sql
lk:tpch_flat_orc_date_1000> explain select sum(l_extendedprice * l_discount) as revenue
				 		 -> from
				 		 -> lineitem
				 		 -> where
				 		 -> l_shipdate >= DATE '1993-01-01'
				 		 -> and l_shipdate < DATE '1994-01-01'
				 		 -> and l_discount between 0.06 - 0.01 and 0.06 + 0.01
				 		 -> and l_quantity < 25;
				 							Query Plan
------------------------------------------------------------------------------------------------------
Output[revenue]
│ Layout: [sum:double]
│ Estimates: {rows: 4859991664 (40.74GB), cpu: 246.43G, memory: 86.00GB, network: 45.26GB}
│ revenue := sum
└─ Aggregate(FINAL)
│ Layout: [sum:double]
│ Estimates: {rows: 4859991664 (40.74GB), cpu: 246.43G, memory: 86.00GB, network: 45.26GB}
│ sum := sum(sum_4)
└─ LocalExchange[SINGLE] ()
│ Layout: [sum_4:double]
│ Estimates: {rows: 5399990738 (45.26GB), cpu: 201.17G, memory: 45.26GB, network: 45.26GB}
└─ RemoteExchange[GATHER]
│ Layout: [sum_4:double]
│ Estimates: {rows: 5399990738 (45.26GB), cpu: 201.17G, memory: 45.26GB, network: 45.26GB}
└─ Aggregate(PARTIAL)
│ Layout: [sum_4:double]
│ Estimates: {rows: 5399990738 (45.26GB), cpu: 201.17G, memory: 45.26GB, network: 0B}
│ sum_4 := sum(expr)
└─ ScanProject[table = hive:tpch_flat_orc_date_1000:lineitem offload={ filter=[AND(AND(BETWEEN(l_discount, 0.05, 0.07), LESS_THAN(l_quantity, 25.0)), AND(GREATER_THAN_OR_EQUAL(l_shipdate, 8401), LESS_THAN(l_shipdate, 8766)))]} ]
 Layout: [expr:double]
 Estimates: {rows: 5999989709 (50.29GB), cpu: 100.58G, memory: 0B, network: 0B}/{rows: 5999989709 (50.29GB), cpu: 150.87G, memory: 0B, network: 0B}
 expr := (l_extendedprice) * (l_discount)
 l_extendedprice := l_extendedprice:double:5:REGULAR
 l_discount := l_discount:double:6:REGULAR
```

## OmniData连接器限制

- 需要在存储节点上部署OmniData服务。

- 仅支持Filter、Aggregator、Limit算子的下推。

  