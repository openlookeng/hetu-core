
# TPCDS连接器

TPCDS连接器提供一组模式来支持TPC Benchmark™ DS (TPC-DS)。TPC-DS是一个数据库基准，用于衡量复杂决策支持数据库的性能。

此连接器还可以用于测试openLooKeng的功能和查询语法，而无需配置对外部数据源的访问。当查询TPCDS模式时，连接器使用确定性算法动态生成数据。

## 配置

要配置TPCDS连接器，创建一个具有以下内容的目录属性文件`etc/catalog/tpcds.properties`：

``` properties
connector.name=tpcds
```

## TPCDS模式

TPCDS连接器提供了几个模式：

    SHOW SCHEMAS FROM tpcds;

```
Schema
--------------------
information_schema
sf1
sf10
sf100
sf1000
sf10000
sf100000
sf300
sf3000
sf30000
tiny
(11 rows)
```

忽略标准模式`information_schema`，该模式每个目录中都存在，且不是由TPCDS连接器直接提供的。

每个TPCDS模式都提供相同的一组表。有些表在所有模式中都是相同的。特定模式中表的比例系数由模式名称确定。例如模式`sf1`对应比例系数`1`，模式`sf300`对应比例系数`300`。比例系数中的每一个单位都对应于1 GB的数据。例如，对于比例系数`300`，将生成总计`300` GB。`tiny`模式是比例系数`0.01`的别名，该模式是用于测试的一个非常小的数据集。