
# Deploying openLooKeng with High Availability(HA)

The openLooKeng HA solves single point failure of coordinators. Users can submit queries to any coordinator to balance the workloads.

## Installing HA
openLooKeng with HA is required to be installed with minimum of 2 coordinators in the cluster.
Please follow [Deploying openLooKeng Manually](./deployment.md) or [Deploying openLooKeng Automatically](./deployment-auto.md) for basic installation.

## Configuring HA

- Create an `etc\state-store.properties` file inside both coordinators and workers installation directories.


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

-  `hetu.multiple-coordinator.enabled`: Multiple coordinator mode is enabled
-  `hetu.embedded-state-store.enabled`: Allow this coordinator to use embedded state store

## HA Cluster behind Reverse Proxy

To achieve the full benefit of HA, clients(ie. openLooKeng CLI, JDBC, etc) are encouraged to _not_ connect to specific coordinators directly. Instead, they should connect to multiple coordinators through some sort of _reverse proxy_, for example through a load balancer, or a Kubernetes service. This allows the client to continue to work even if a specific coordinator is not working as expected.

### Reverse Proxy Requirement

When the connection is made through a reverse proxy, it is required for a given client to connect to the same coordinator during the execution of a query. This is to ensure a constant heartbeat between the client and coordinator while that query is running. This can be achieved by enabling _sticky_ connections, for example, Nginx's `ip_hash`.

### Configure Reverse Proxy Example (Nginx)

Please include configuration below in the Nginx configuration file (ie. `nginx.conf`)

```
http {
    ...  # Your own configuration
    upstream backend {
        ip_hash;
        server <coordinator1_ip>:<coordinator1_port>;
        server <coordinator2_ip>:<coordinator2_port>;
        server <coordinator3_ip>:<coordinator3_port>;
        ...
    }

    server {
        ... # Your own configuration
        location / {
            proxy_pass http://backend;
            proxy_set_header Host <nginx_ip>:<nginx_port>;
        }
    }
}
```