# Release 1.7.0

## Key Features

| Area                  | Feature                                                      |
| --------------------- | ------------------------------------------------------------ |
| Operator processing extension            | Users can customize the physical execution plan of worker nodes. Users can use their own operator pipelines to replace the native implementation to accelerate operator processing. |
| Task Recovery         | 1. Enables query recovery by restarting the query, without capturing snapshots during query execution<br/> 2. Support snapshot related information display in CLI debug mode: Number of snapshots being captured currently/ Number of snapshots captured already/ Number of times query recovered (either by resuming or restarting)/ Size of the snapshots captured and Time taken to capture snapshots/ Size of snapshots restored and time taken to restore<br/> 3. Resource Manager: Suspend a query on low memory situation which is consuming most memory and resume. User can suspend/resume a query（limit sql in CTAS and INSERT INTO SELECT）<br/> 4. Add customizable failure retry policies: Timeout based retry policy and Max-retry based retry policy<br/> 5. Add Gossip based failure detection mechanism: Coordinator continuously monitors the workers whether they are alive by receiving periodic heartbeats. Gossip mechanism enables all workers to monitor each other, and let the coordinator know about their local knowledge on each other’s availability. Gossiping can prevent premature query failing in case of transient network failures in the cluster. |
| openLooKeng Web UI Enhancement         | 1. openLooKeng audit log feature enhancements<br/> 2. openLooKeng SQL history query feature enhancements<br/> 3. SQL Editor supports automatic association<br/> 4. Supports the collection of common sql statements<br/> 5. When a data source loads an exception, the front end can be displayed through exception tags and does not affect the normal operation of the openLooKeng service<br/> 6. The UI interface supports displaying specific information about the data source(Show catalog). |
| openLooKeng Cluster Management | 1. Provides graphical indicator monitoring and customization to obtain key system information<br/> 2. Provides configuration management of service attributes to meet actual service performance requirements<br/> 3. Provides operations on clusters, services, and instances to meet one-click start and stop requirements<br/> 4. Provides the openLooKeng online deployment function and supports online upgrade<br/> 5. Provides the node scaling service<br/> 6. Integrate the existing web UI of openLooKeng<br/> 7. The openLooKeng Manager tool supports user login and logout. |

## Known Issues

| Category      | Description                                                  | Gitee issue                                               |
| ------------- | ------------------------------------------------------------ | --------------------------------------------------------- |
| Task Recovery | When a snapshot is enabled and a CTAS with transaction is executed, kill one worker, an error is reported in the SQL statement. | [I5EAM3](https://e.gitee.com/open_lookeng/issues/list?issue=I5EAM3) |


## Obtaining the Document 

For details, see [https://gitee.com/openlookeng/hetu-core/tree/1.7.0/hetu-docs/en](https://gitee.com/openlookeng/hetu-core/tree/1.7.0/hetu-docs/en)

