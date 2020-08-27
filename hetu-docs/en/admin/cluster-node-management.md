

# Cluster Node Management

There can potentially be multiple nodes (coordinators and workers) in a openLooKeng cluster. Sometimes admin operations need to be performed on these nodes, for scaling or maintenance purposes.

## Shutdown a node

If a cluster does not need a node anymore (e.g. as part of a scaling down operation), the node can be shutdown.

Such process is _graceful_, in that
- the cluster does not assign new workload to this node, and
- the node attempts to finish all existing workloads before shutting down its main process.

The shutdown process is _irreversible_. It can be initiated through the REST API, by putting the `SHUTTING_DOWN` state to the `info/state` endpoint of the node. For example, with `curl` and assuming the node to shutdown has address `1.2.3.4:8080`:

```sh
curl -X PUT -H "Content-Type: application/json" http://1.2.3.4:8080/v1/info/state \
    -d '"SHUTTING_DOWN"'
```

## Isolate a node

_Isolation_ takes a node off of the cluster _temporarily_, for example to perform maintenance tasks on it. The node can be added back to the cluster afterwards.

- Isolation can be graceful in a way similar to shutdown: wait for workloads to finish first. Nodes in this waiting period are in the `isolating` state.
- It can also be non-graceful, to enter the `isolated` directly.
- `Isolating` and `isolated` nodes are not assigned new workloads.

Nodes' isolation status are changed via the REST API, with different target states. For example:

```sh
# To gracefully isolate a node
curl -X PUT -H "Content-Type: application/json" http://1.2.3.4:8080/v1/info/state \
    -d '"ISOLATING"'

# To isolate a node immediately
curl -X PUT -H "Content-Type: application/json" http://1.2.3.4:8080/v1/info/state \
    -d '"ISOLATED"'

# To make the node available again
curl -X PUT -H "Content-Type: application/json" http://1.2.3.4:8080/v1/info/state \
    -d '"ACTIVE"'
```

## Node state transitions

A openLooKeng node (coordinator or worker) can be in one of 5 states:
- Inactive
- Active
- Isolating
- Isolated
- Shutting down

Shutdown and isolation operations move nodes among these states. The following table shows which transitions are allowed, where "-" indicates that the transition is allowed but has no effect.

|from\to|inactive|active|isolating|isolated|shutting_down|exit|
|-|-|-|-|-|-|-|
|inactive| | | | | 5 | |
|active| | - | 2 | 3 | 5 | |
|isolating| | 1 | - | 3, 4 | 5 | |
|isolated| | 1 | 2 | - | 5 | |
|shutting_down| | | | | - | 6 |

Transition details:
1. Restore the node back to normal operation
1. Request to isolate the node, by not assigning new workload on it, and waiting for existing workloads to finish
1. Immediately isolate the node, by not assigning new workload. If there are existing workloads, they are deemed unimportant and may not finish
1. Automatic transition when a node is `ISOLATING` and active workloads finish
1. Request to shut down the node, by not assigning new workload on it, and waiting for existing workloads to finish
1. Automatic transition when a node is `SHUTTING_DOWN` and active workloads finish
