# singleData连接器

singleData连接器为openGauss补充OLAP能力，增强数据分析竞争力。

singleData分为ShardingSphere和tidRange两种模式。

## ShardingSphere模式

### 概述

Apache ShardingSphere是一款分布式数据库生态系统，它可以将任意数据库转换为分布式数据库，singleData连接器可以使用ShardingSphere对openGauss的数据进行分片，以提升数据分析效率。

要了解更多shardingsphere信息请见：[ShardingSphere官方网站](https://shardingsphere.apache.org/)

### 配置

要配置ShardingSphere模式的singleData连接器，在`etc/catalog`中创建一个目录属性文件，例如`shardingsphere.properties`，使用以下内容创建文件，并替换相应的属性。

- 基本配置
``` properties
connector.name=singledata
singledata.mode=SHARDING_SPHERE
shardingsphere.database-name=sharding_db
shardingsphere.type=zookeeper
shardingsphere.namespace=governance_ds
shardingsphere.server-list=localhost:2181
```

- 属性说明

| 属性名称                         | 属性说明                                     | 是否必要 |
|------------------------------|------------------------------------------|------|
| shardingsphere.database-name | 连接的shardingsphere database的名称            | 是    |
| shardingsphere.type          | 注册中心持久化仓库类型，支持zookeeper和etcd，推荐zookeeper | 是    |
| shardingsphere.namespace     | 注册中心命名空间                                 | 是    |
| shardingsphere.server-lists  | 注册中心连接地址                                 | 是    |

- 仓库类型为zookeeper的可选配置

| 属性名称                                                    | 说明           | 默认值 |
|---------------------------------------------------------|--------------|-----|
| shardingsphere.zookeeper.retry-interval-milliseconds    | 连接失败后重试间隔毫秒数 | 500 |
| shardingsphere.zookeeper.max-retries                    | 连接最大重试数      | 3   |
| shardingsphere.zookeeper.time-to-live-seconds           | 临时节点存活秒数     | 60  |
| shardingsphere.zookeeper.operation-timeout-milliseconds | 操作超时毫秒数      | 500 |
| shardingsphere.zookeeper.digest                         | 权限令牌         | ""  |

- 仓库类型为etcd的可选配置

| 属性名称                                     | 说明       | 默认值 |
|------------------------------------------|----------|-----|
| shardingsphere.etcd.time-to-live-seconds | 临时节点存活秒数 | 30  |
| shardingsphere.etcd.connection-timeout   | 连接超时秒数   | 3   |

**说明**

- 目前singleData连接器只支持5.2.0版本的ShardingSphere,其他版本暂时不支持

## TidRange模式

### 概述

openGauss的数据是按行存储在HEAP PAGE中，每一行数据都会有对应的ctid（即行号）。openGauss的tidrangescan插件可以使用ctid的范围来进行查询，singleData连接器的tidRange模式利用这个插件来实现数据的并行分析。

tidrangescan插件获取地址：[Plugin](https://gitee.com/opengauss/Plugin)

### 连接配置

要配置tidRange模式的singleData连接器，在`etc/catalog`中创建一个目录属性文件，例如`tidrange.properties`。使用以下内容创建文件，并根据设置替换连接属性
```properties
connection.name=singledata
connection.mode=TID_RANGE
connection-url=jdbc:opengauss://master-host:port/database;jdbc:opengauss://slave-host:port/database
connection-user=user
connection-password=password
```
- connection-url可以配置多个主备节点的jdbc连接地址，地址间以`;`作为分隔符，在进行查询时，每个分片会随机选择一个连接地址进行连接
- 其他连接配置请参考openGauss连接器

### 分片配置

| 属性名称                        | 属性说明                                                          | 是否必须 | 默认值  |
|-----------------------------|---------------------------------------------------------------|------|------|
| tidrange.max-split-count    | 最大分片数量，即最大jdbc连接数，这个数值应该不大于openGauss的max_connections配置        | 否    | 100  |
| tidrange.page-size          | openGauss的page大小，请确保这个配置和openGauss的block_size一致，否则可能会导致查询结果错误 | 否    | 8kB  |
| tidrange.default-split-size | 默认的每个分片的大小。当数据量较小时，singleData连接器会按此配置进行分片，配置的范围为1MB-1GB       | 否    | 32MB |

**说明**
- 本特性需配合openGauss的tidrangescan插件使用，没有tidrangescan插件singledata连接器也可以正常完成查询功能，但会导致性能大幅下降
- 当查询的表中存在索引时，将不会启用tidrange功能

## 限制说明

- singleData连接器目前只提供对openGauss的查询功能，暂时不支持INSERT/UPDATE/DELETE等会修改数据或者数据结构的语句，查询功能请参考openGauss的连接器
- openLookeng的Decimal精度最高支持为38，当openGauss的decimal，numeric类型的精度超过38时则无法支持
- openGauss版本支持3.0.0及以上