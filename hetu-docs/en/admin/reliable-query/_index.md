
# Overview

When a node in a cluster fails as result of network, hardware, or software issues, all queries with tasks running on the failing node will be lost. This can significantly impact cluster productivity and waste precious resources, especially for long running queries.

One way to overcome this is to automatically rerun those impacted queries. This reduces the need for human intervention and increases fault tolerance, but as a result, the total execution time can be much longer.

To achieve better performance while maintaining execution reliability, there are two mechanisms in openLooKeng provided:

- [Operator Snapshot](./operator-snapshot.md)
- [Task Snapshot](./task-snapshot.md)


