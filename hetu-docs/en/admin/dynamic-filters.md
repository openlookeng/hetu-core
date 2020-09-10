# Dynamic Filtering
This section describes the openlookeng dynamic filtering features. Dynamic filtering is suitable for highly selective join scenarios, that is, most of the tables on the probe side are filtered out after reading because they do not match the join conditions.

Openlookeng generates dynamic filter conditions based on join conditions and data read from build side during query run, and is applied to the table scan stage of probe side table as an additional filter condition, to reduce the data volume of probe table participating in join operation and effectively reduce IO read and network transmission.

## scenarios
Openlookeng dynamic filtering is currently applicable to 'inner join' and 'right join' scenarios, only can be applied to 'hive connector' and 'DC connector'.

## Usage
Openlookeng's dynamic filtering feature depends on the distributed cache component. Please refer to the section [Configuring HA](../installation/deployment-ha.md) to configure 'hazelcast'.
In '/ etc/ config.properties "The following parameters need to be configured.

``` properties
enable-dynamic-filtering=true
dynamic-filtering-data-type=BLOOM_FILTER
dynamic-filtering-max-per-driver-size='100MB'
dynamic-filtering-max-per-driver-row-count=10000
dynamic-filtering-bloom-filter-fpp=0.1
```

The above attributes are described below：

- `enable-dynamic-filtering`：Enable dynamic filtering feature.
- `dynamic-filtering-data-type`：Set dynamic filtering data type, default to BLOOM_FILTER.
- `dynamic-filtering-max-per-driver-size`：Max data size collected for dynamic filter per driver, default to 10KB.
- `dynamic-filtering-max-per-driver-row-count`：Max data count collected for dynamic filter per driver, default to 100.
- `dynamic-filtering-bloom-filter-fpp`：Bloom filter FPP used for dynamic filter, default to 0.1.

Session properties
``` properties
dynamic_filtering_wait_time='200ms'
```

The above attributes are described below:
- `dynamic_filtering_wait_time`：Maximum waiting time for the dynamic filter to be ready, default to 0ms.

If applied to `Hive connector`，we should change `catalog/hive.properties`：
``` properties
hive.dynamic-filter-partition-filtering=true
hive.dynamic-filtering-row-filtering-threshold=5000
```

The above attributes are described below:
- `hive.dynamic-filter-partition-filtering`：Filter out hive splits early based on partition value using dynamic filter, default to false.
- `hive.dynamic-filtering-row-filtering-threshold`：Filter out hive rows early if the dynamic filter size is below the threshold, default to 2000.

