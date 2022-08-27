
# Task Snapshot

## Overview

Task Snapshot is another mechanism for reliable query, it is a trade-off of Retry Query and Reliable Query Execution by Operator Snapshot.
Compared with Retry Query, query can be resumed through task snapshot instead of re-execution after a failure occurs, thereby optimizing the recovery time.
Compared with Reliable Query Execution by Operator Snapshot, the granularity space usage of task snapshot is smaller than that of operator snapshot.

By default, a query needs to be retried manually after the query fails. Task Snapshot in openLooKeng is used to ensure reliable query execution by resuming automatically.
After the Task Snapshot is enabled, the exchange data between tasks are materialized through snapshots during the worker in query process.
When the coordinator in query process detects that worker is faulty, or the query failures, the coordinator schedules the idle worker to read the last saved task snapshot by the faulty worker, and then automatically resume query execution.
This can ensure the success of long-running queries, therefore, users don't need to watch over the long-running queries all the time in case there might be a failure, which enables openLooKeng for batch scenarios.

![](../../images/task_snapshot_overview.png)

## Enable Recovery framework

Recovery framework is most useful for complex queries that need long-running time. It is disabled by default, and must be enabled or disabled via a configuration property `retry-policy` in the configuration file `etc/config.properties`.
- **Enable**: `retry-policy=TASK`
- **Disable**: `retry-policy=NONE`

## Requirements

To ensure that query execution can be automatically resumed through a previously saved task snapshot and query execution is reliable, the cluster needs to meet the following requirements:
- the recovery framework is enabled
- the cluster contains at least 2 worker nodes
- the surviving worker nodes in the cluster are still active

## Limitations

- **Supported Statements**: only `SELECT`, `INSERT` and `CREATE TABLE AS SELECT(CTAS)` types of statements are supported
   - This does *not* include statements like `INSERT INTO CUBE`
- **Source tables**: can only read from tables in `Hive` catalog
- **Target table**: can only write to tables in `Hive` catalogs, with `ORC` format
- **Interaction with other features**: Task Snapshot feature and Operator Snapshot feature do not support the same time

## Detection

The recovery query execution is triggered when the communication between the coordinator and the worker in query process failure or times out. The retry configuration of the relevant fault recovery properties is as follows:
- property: `task-retry-attempts-per-task`
  - default value: 4
  - description: Maximum number of times may attempt to retry a single task before declaring the query as failed.
- property: `task-initial-delay`
  - default value: 10s
  - description: Minimum time that a failed task must wait before it is retried.
- property: `task-max-delay`
  - default value: 1m
  - description: Maximum time that a failed task must wait before it is retried.

## Storage Considerations

The exchange data and status between tasks in worker nodes are materialized to the file system as task snapshot when the query executes, when the file system supports two types: local and hdfs.
When a failure occurs, the coordinator in query process schedules other idle worker nodes to read task snapshot from the specified file directory in the file system, such as to implement reliable query execution.
Therefore, all worker nodes are required to be able to access the file system that stores task snapshots. The exchange manager is adopted to manage file system related properties in the configuration file etc/exchange-manager.properties.
Related properties are as follows:
- property: `exchange-manager.name`
  - description: The name of exchange manager is configured by the property.
- property: `exchange-filesystem-type`
  - description: The type name of file system client used by the task snapshot. If `exchange-filesystem-type=exchange`, the client named `exchange` will be used to store the Task snapshot during query execution. The configuration file path for file system clients is `etc/filesystem/exchange.properties` (the properties file name needs to be consistent with exchange-filesystem-type), see [Exchange File System Client Properties Reference](<../properties.md#exchange-file-system-client>).
- property: `exchange.base-directories`
  - description: The property specifies the URI shared by the file system and accessible to all worker nodes. All task snapshots are stored under the current URI.
- property: `exchange.compression-enabled`
  - description: The property enables whether to compress task snapshots when they are stored.

Note that task snapshots are used to save the exchange data and status between tasks in worker nodes during query execution. ensure that the cluster has enough free storage space to store these task snapshots before query execution, and the site is automatically cleaned to release storage space after query finished.


## Performance Overhead

The ability to recover from an error and resume from a snapshot does not come for free. Capturing a snapshot, depending on complexity, takes time. Thus it is a trade-off between performance and reliability.

It is suggested to turn on snapshot capture when necessary, i.e. for queries that run for a long time. For these types of workloads, the overhead of taking snapshots becomes negligible.


## Configurations

Configurations related to recovery framework feature can be found in [Properties Reference](<../properties.md#fault-tolerant-execution>).