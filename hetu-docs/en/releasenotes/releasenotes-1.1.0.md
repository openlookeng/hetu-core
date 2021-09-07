# Release 1.1.0 (30 Dec 2020)

## Key Features

| Area             | Feature                                                      | PR #s                           |
| ---------------- | ------------------------------------------------------------ | ------------------------------- |
| Heuristic Index  | Btree index – BTree index is used for split filtering, and is used only by the coordinator nodes. If an index is created on a column which is part of a predicate in the query, then openLooKeng may be able to improve the performance of the query by filtering out splits during scheduling time. | 392,437,452,457                 |
|                  | The bitmap index is used for filtering data read from ORC files, and is only used by the worker nodes. The bitmap index should be created on high-cardinality columns. In this release, the bitmap index is enhanced by storing the keys in a BTree for faster key lookup, index creation, and smaller index sizes. | 437,418,435,407,390             |
| High performance | Several performance improvements where made such as: <br/>·     Improved the performance of the window function.<br/>·     Changed the dynamic filter wait time to be based on the time the task was scheduled on the worker, and to not include the time spent in planning.<br/>·     Apply dynamic filters to semi-joins.<br/>·     Implemented a rule to convert left join to semi-join.<br/>·     Implemented a rule to convert the self-join to a group by. | 429,366,385,406,391,330,446,382 |
|                  | Having many small splits affects the scheduling in openLooKeng as it need to do time slice to multiple splits. This causes more scheduling overhead as more times goes in switching across splits and less work done and the wait time of each split to be read also increases due to this. In order to avoid this, Merge Small splits feature is used to group the small splits and schedule together as though it’s a single split scheduling | 387                             |
|                  | Reuse exchange feature introduces a query optimizer to reduce the query latency by caching table data in memoery when it is used more than once in a query with the same projections and filters applied to it | 443                             |
| Connector        | Support openGauss data source                                | 492                             |
|                  | Support Mongodb data source                                  | 464                             |
|                  | Refactor ES connector to be compatible with ES 7.X           | 380                             |
|                  | Introduce the document for Oracle connector, and enhance Oracle connector to support synonyms. | 380,487                         |
|                  | Enhance the hive connector to support impersonation in Hive  Metastore. | 454                             |
|                  | Improve the SQL syntax compatibility of ODBC driver.         | 15,10,19,12,21,22               |
| DC connector     | Support dynamic filter push down in cross data center connector | 402,419                         |
| User experience  | Provide a brand new admin web UI with SQL editor and system monitor | 163,368,404                     |
|                  | Simplify the configurations                                  | 397,369,449                     |
| Security         | Support integration with Apache Ranger for unified permission control | 491                             |


## Known Issues

| Category          | Description                                                  | Gitee issue                                                  |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Data Center Connector | If the Data center has many more than 3 tiers, and when the Join condition is on a small value, enabling dynamic filtering takes longer time than disabling dynamic filtering. This is because the column data is not high-cardinality, we need a better filter. | [I2BAZZ](https://gitee.com/openlookeng/hetu-core/issues/I2BAZZ) |
| Heuristic Index       | After drop index ,the index files folders would not be removed from HDFS. However the files inside them are removed | [I2BB1N](https://gitee.com/openlookeng/hetu-core/issues/I2BB1N) |
|                       | Creating Index with or-predicate shows the incorrect unsupported error message. | [I2BB3O](https://gitee.com/openlookeng/hetu-core/issues/I2BB3O) |
|                       | The splits cannot be filtered after create BTree index  for transactional partition table | [I2BB6M](https://gitee.com/openlookeng/hetu-core/issues/I2BB6M) |
| WEB UI                | The response time of the metadata tree list is long  after the refresh button is clicked.This is because the cache is invalidated and the data is re-fetched from all catalogs and schemas for all tables and columns. | [I2BB2B](https://gitee.com/openlookeng/hetu-core/issues/I2BB2B) |
|                       | The text is overflow out from the text area when there  are too much live plans information. | [I2BB4E](https://gitee.com/openlookeng/hetu-core/issues/I2BB4E) |
| Installation          | By using the auto deploy script, the system reused the old configuration file without any prompt messages, this will mislead user. | [I2BB52](https://gitee.com/openlookeng/hetu-core/issues/I2BB52) |
| Vacuum                | When a vacuum is run more than once on a table which had delete/update operations performed on it, the hive statistics might get corrupted and the queries would fail. To avoid this, set the session flag hive.collect_column_statistics_on_write to false (`set session hive.collect_column_statistics_on_write=false`) before running VACUUM command. In case the command was run without setting the above flag, run ANALYZE `table name` command to rectify the statistics. This issue is rectified in PR 517, but not merged into 1.1.0 release. | [I2BFH9](https://gitee.com/openlookeng/hetu-core/issues/I2BFH9) |
| Reuse Exchange        | When reuse exchange feature is enabled in config.properties (reuse_table_scan=true), querying non hive catalogs would fail. It is recommended use (set session reuse_table_scan=true;) when querying hive catalogs and disable it for other catalogs. This issue is rectified in PR 516, but not merged into 1.1.0 release. | [I2BEWV](https://gitee.com/openlookeng/hetu-core/issues/I2BEWV) |

## Obtaining the Document 

For details, see [https://gitee.com/openlookeng/hetu-core/tree/1.1.0/hetu-docs/en](https://gitee.com/openlookeng/hetu-core/tree/1.1.0/hetu-docs/en)