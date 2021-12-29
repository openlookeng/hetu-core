# Release 1.5.0

## Key Features

| Area             | Feature                                                      |
| ---------------- | ------------------------------------------------------------ |
| Star Tree        | 1. Support for optimizing join queries such as star schema queries.<br/>2. Optimized the query plan by eliminating unnecessary aggregations on top of the cube since the cube already contains the rolled up results. The performance optimizations benefits queries whose group by clause exactly matches the cube’s group.<br/>3. Bug fixes to further enhance the usability, and robustness of cubes. |
| Memory Connector | 1. Improved performance of memory connector by adding support for memory table partitioning to allow data skipping of entire partitions<br/>2. Collect statistics on memory table to support openLooKeng cost based optimizers. |
| Task Recovery    | Fixed several important bugs to address data inconsistency issues, and query hanging issues that occasionally occur during high concurrency, and during worker failures. |
| OLK-on-Yarn      | Support deploying an HA-enabled openLooKeng cluster instance on-yarn, that contains a reverse proxy (ngnix by default), and 2 or more coordinator nodes. The cluster can be horizontally scaled manually by adding and removing yarn containers to the coordinator and worker components.|
| Spill to Disk    | Optimized the spill to disk mechansim to directly write Pages to disk instead of buffering it. Changed the strategy to spill those operators which can free up maximum memory. This resulted in improvement of spill to disk performance by 30% |

## Known Issues

| Category              | Description                                                  | Gitee issue                                                       |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Task Recovery         |When the query process reaches stage1,a worker is killed result some values become smaller(occasionally).   |[I4M2LW](https://e.gitee.com/open_lookeng/issues/list?issue=I4M2LW) |
| Memory Connector      |When a table with the index_columns parameter is queried, an error message is displayed：java.lang.NullPointerException. | [I4NVW3](https://e.gitee.com/open_lookeng/issues/list?issue=I4NVW3)|
|                       |When drop and then create a same partitioned table with data type double, query result rows is greater than expected. | [I4LUDF](https://e.gitee.com/open_lookeng/issues/list?issue=I4LUDF) |

## Obtaining the Document 

For details, see [https://gitee.com/openlookeng/hetu-core/tree/1.5.0/hetu-docs/en](https://gitee.com/openlookeng/hetu-core/tree/1.5.0/hetu-docs/en)