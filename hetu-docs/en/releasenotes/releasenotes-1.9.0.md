# Release 1.9.0

## Key Features

| Area                      | Feature                                                                                                                                                                                                                              |
|---------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Task Level Recovery       | Support task retry and recovery in the Create Table As Select scenario.           |
| Connector Extension       | 1. Add support Iceberg connector to add/delete/modify/access data, hidden partitioning, time travel, transaction and small file compaction<br/> 2. Enhance Elastic Search connector to support filter and aggregation pushdown.      |
| Performance Enhancement   | 1. CTE optimizations including reuse execution pipeline and cte results materialization and reuse<br/> 2. Plan optimizations including use exact partitioning, adaptive aggregations, join independence assumptions for selectivity estimates and execution policy for better filtering.                                                                                                |

## Obtaining the Document 

For details, see [https://gitee.com/openlookeng/hetu-core/tree/1.9.0/hetu-docs/en](https://gitee.com/openlookeng/hetu-core/tree/1.9.0/hetu-docs/en)
