# Release 1.0.0 (23 Sep 2020)

## Key Features

This release focused on making improvements in 3 main areas: Performance, Security, and Usability.  

* Performance

    New enhancements were made to the engine to further improve the performance of ad-hoc interactive queries. These enhancements include changes to the dynamic filter feature to use a more efficient implementation of bloom filters, as well as optimizing the dynamic filter source operator so that dynamic filters can be collected and used as soon as possible.  Other performance optimizations include the introduction of  predicate pushdown to allow OR predicates to be pushed to the ORC reader, as well as supporting OR predicates during split filtering with the heuristic index feature. Finally, auto compaction was introduced for ORC files to reduce the number of ORC files after insert and update operations.

* Security

    In this release the community focused on further securing the query engine. Several vulnerabilities were addressed in the code, and an audit log was introduced. Furthermore, a new feature was introduced to allow administrators to encrypt sensitive information like data source credentials found in catalog property files.

* Usability

    Usability was another area of main focus for this release. In order to provide a database-like user experience, the community introduced a migration tool that helps migrate SQL queries from other engines to work in openLooKeng. Other usability enhancements include capturing ORC cache metrics via JMX to allow administrators to view cache hits and misses, and also improving the CACHE sql command to provide more flexibility as to what can be cached. Furthermore, changes were made to installation and deployment scripts to support deployments that contain a mix of ARM and x86 nodes. New scripts are introduced to allow administrators to deploy openLooKeng service based on containers.


| Area                    | Feature                                                      | PR #s                                                        |
| ----------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Security                | Audit Logging                                                | 160                                                          |
|                         | Security Vulnerability Fixes                                 | 113                                                          |
|                         | Handle CVEs                                                  | 144                                                          |
| Installation Deployment | Scripts to help deploy and run openLooKeng on Docker & Kubernetes | 50                                                           |
|                         | Support installation on nodes with mixed architectures of ARM vs. x86. | 5                                                            |
| Tools                   | Support migration of Impala SQL to ANSI SQL                  | 52                                                           |
|                         | Usability enhancements to SQL migration tool to help user to easily identify differences between SQL statements, and accept changes. | 159                                                          |
| ODBC Driver             | Support openLooKeng complex data type such as ARRAY, JSON,MAP, and ROW. | 11 (in hetu-odbc-gateway's repo) & 6 in (hetu-odbc-driver's repo) |
|                         | Bidirectional Scrollable cursors supported so as to enable easier iteration on tables & result sets. Position also can be specified to set the cursor to | 10                                                           |
| Heuristic Index         | Refactor heuristic indexer, change to openLooKeng plugin, use HetuFileSystem Client | 40                                                           |
|                         | Add additional configs to give admin better control of the index cache. For, example, admin can now set the maximum amount of memory the index cache can take. | 85                                                           |
|                         | Support split filtering at schedule time for queries that contain OR predicate. | 93                                                           |
|                         | Add session property to disable heuristic index while the server is running. | 126                                                          |
| Optimizers              | Push predicates such as OR predicates to ORC reader to perform data skipping. | 103                                                          |
| Execution Pipeline      | Several Dynamic filter enhancements such as the use of an internal bloom filter implementation that gives better performance, as well as improved row filtering logic, and added State Store listeners to collect newly merged dynamic filters. | 65, 86, 100, 112, 137                                        |
|                         | Enhanced dynamic filters to support more types of blocks, and enabled block level filtering to take advantage of vectorization. | 101, 117                                                     |
| ORC IUD/ACID            | Supported threshold based auto triggering and handling of compaction so that user intervention is not required and the query performance doesn't degrade when mutation operations are performed on the table (Currently supported for Hive) | 141                                                          |
|                         | Auto cleanup of ORC files after compaction operation.        | 89                                                           |
| Catalog Management      | Encryption of sensitive information like passwords in catalog properties file. | 170                                                          |
| DC Connector            | Improved the performance of DC connector by supporting Metadata & plan cache. | 128                                                          |
| ORC Data Cache          | Capture ORC cache metrics via JMX.                           | 33, 142                                                      |
|                         | CACHE sql command usability enhancements to provide more flexibility as to what partitions can be cached. | 90, 99                                                       |


## Obtaining the Document 

For details, see [https://gitee.com/openlookeng/hetu-core/tree/1.0.0/hetu-docs/en](https://gitee.com/openlookeng/hetu-core/tree/1.0.0/hetu-docs/en)


