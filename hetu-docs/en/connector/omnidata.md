
OmniData Connector
==============

## Overview

The OmniData connector allows querying data stored in the remote Hive data warehouse.
It pushes the operators of openLooKeng down to the storage node to achieve near-data calculation, thereby reducing the amount of network transmission data and improving computing performance.

For more information, please see: [OmniData](https://www.hikunpeng.com/en/developer/boostkit/big-data?accelerated=3) and [OmniData connector](https://github.com/kunpengcompute/omnidata-openlookeng-connector).

## Supported File Types


The following file types are supported for the OmniData connector:

-   ORC
-   Parquet
-   Text

## Configuration

Create `etc/catalog/omnidata.properties` with the following configurations, replacing `example.net:9083` with the correct host and port for your Hive metastore Thrift service:

``` properties
connector.name=omnidata-openlookeng
hive.metastore.uri=thrift://example.net:9083
```

### HDFS Configuration

For basic setups, openLooKeng configures the HDFS client automatically and does not require any configuration files. In some cases, such as when using federated HDFS or NameNode high availability, it is necessary to specify additional HDFS client options in order to access your HDFS cluster. To do so, add the `hive.config.resources` property to reference your HDFS config files:

``` properties
hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml
```

Only specify additional configuration files if necessary for your setup. We also recommend reducing the configuration files to have the minimum set of required properties, as additional properties may cause problems.

The configuration files must exist on all openLooKeng nodes. If you are referencing existing Hadoop config files, make sure to copy them to any openLooKeng nodes that are not running Hadoop.

## OmniData Configuration Properties

| Property Name                   | Description                                                  | Default |
| ------------------------------- | ------------------------------------------------------------ | ------- |
| hive.metastore                  | The type of Hive metastore                                   | thrift  |
| hive.config.resources           | An optional comma-separated list of HDFS configuration files. These files must exist on the machines running openLooKeng. Only specify this if absolutely necessary to access HDFS. Example: `/etc/hdfs-site.xml` |         |
| hive.omnidata-enabled           | Allows push-down operators to execute on the storage side. If disabled, all operators will not be pushed down. | true    |
| hive.min-offload-row-number     | If the number of rows in the table is less than the threshold, all operators of the table will not be pushed down. | 500     |
| hive.filter-offload-enabled     | Allows the filter operator to be pushed down to the storage side. If disabled, the filter operator will not be pushed down. | true    |
| hive.filter-offload-factor      | Only when the selection rate of the filter operator is less than the threshold, it will be pushed down. | 0.25    |
| hive.aggregator-offload-enabled | Allows the aggregator operator to be pushed down to the storage side. If disabled, the aggregator operator will not be pushed down. | true    |
| hive.aggregator-offload-factor  | Only when the aggregation rate of the aggregator operator is less than the threshold, it will be pushed down. | 0.25    |

For more configuration, please refer to the [Hive Configuration Properties](./hive.md#Hive Configuration Properties) chapter.

### Querying OmniData

The SQL query plan after some operators are pushed down:

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


## OmniData Connector Limitations


- The OmniData service needs to be deployed on the storage node.
- Only the pushdown of Filter, Aggregator, and Limit operators are supported.
