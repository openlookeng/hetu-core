+++

weight = 8
title = "Deploying openLooKeng with High Availability(HA)"
+++

# Deploying openLooKeng with High Availability(HA)

The openLooKeng HA solves single point failure of coordinators. Users can submit queries to any coordinator to balance the workloads.

## Installing HA
openLooKeng with HA is required to be installed with minimum of 2 coordinators in the cluster.
Please follow [openLooKeng Manual Setup](deployment.md) or [openLooKeng Auto Setup](deployment-auto.md) for basic installation.

## Configuring HA

- Create an ``etc\state-store.properties`` file inside both coordinators and workers installation directories.


### State Store Properties

The state store properties file, `etc/state-store.properties`, contains configuration to state store.
State store is used to store states that are shared between coordinators and workers. The following is a minimal configuration.

``` properties
state-store.type=hazelcast
state-store.name=query
state-store.cluster=cluster1
```

The above properties are described below:

- `state-store.type`: The type of the state store. For now, only support hazelcast
- `state-store.name`: User defined name of state store
- `state-store.cluster`: User defined cluster name of state store

### Coordinator and Worker Properties

Add following configuration to `etc/config.properties` on all coordinators.

``` properties
hetu.multiple-coordinator.enabled=true
hetu.embedded-state-store.enabled=true
```

Add following configuration to ``etc/config.properties`` on all workers.

``` properties
hetu.multiple-coordinator.enabled=true
```

The above properties are described below:

-  hetu.multiple-coordinator.enabled`: Multiple coordinator mode is enabled
-  hetu.embedded-state-store.enabled`: Allow this coordinator to use embedded state store
