ORC Cache
=========
ORC Cache feature improves the query performance by caching frequently accessed data. ORC Cache reduces time spent on TableScan operation because 
the network IO This in turn reduces the query latency.   

This feature is most beneficial for caching raw data from tables that are most frequently accessed and not co-located with
the openLooKeng deployment. If enabled, workers automatically cache file tail, stripe footer, row index, bloom index 
of all ORC files because they are small. However, row group data tends to be huge and caching row group data for all 
files in not practically feasible because of the limitation with cache size.

Check this [link](https://orc.apache.org/specification/ORCv1/) to know about ORC specification.

``CACHE TABLE`` SQL command can be used to configure the table and partition for which row data should be cached by the Worker. 

The following sections briefly explains how the entire row data cache implementation works.

SplitCacheMap
-------------
Users can use `CACHE TABLE` sql statement to configure which table and data must be cached by Hive connector. The partitions to cache are defined 
as predicates and are stored in `SplitCacheMap`. SplitCacheMap is stored in local memory of the coordinator.

Sample query to cache sales table data for days between 2020-01-04 and 2020-01-11.

  `cache table hive.default.sales where sales_date BETWEEN date '2020-01-04' AND date'2020-01-11'`

Check `CACHE TABLE`, `SHOW CACHE`, and `DROP CACHE` commands for more information.

SplitCacheMap stores two kinds of information
  1. Table name along with predicates provided via `CACHE TABLE` command.
  2. Split to Worker mapping

Connector
---------
When caching is enabled and a predicate is provided through ``CACHE TABLE`` SQL command, HiveSplits will be flagged by the connector 
as cacheable if the corresponding partitioned ORC file matches the predicate. 

SplitCacheAwareNodeSelector
---------------------------
 SplitCacheAwareNodeSelector is implemented to support cache affinity scheduling.  SplitCacheAwareNodeSelector is like any other node selector 
 responsible for assigning splits to workers. When a split is scheduled for first time, the node selector stores the split and worker on which 
 the split was scheduled. For subsequent scheduling, this information is used to determine whether split has already been processed by a worker.
 If so, the node selector schedules the split on the worker that previously processed it. If not, SplitCacheAwareNodeSelector falls back to default 
 node selector to schedule the split.  Workers which process the splits will cache the data mapped by the split in local memory.
 
Workers
-------
Workers rely on `ConnectorSplit.isCacheable` method to determine whether split data must be cached. If property is set
to true, the HiveConnector tries to retrieve the data from Cache. In case of cache miss, the data is read from HDFS and stored in Cache for future
use. Workers will purge their caches by expiry time or by reaching size limit, independently of the coordinator.

Check `ORC Cache Configuration` under Hive connector to know more about cache config.
