
# Deploying openLooKeng with High Availability(HA)

The openLooKeng HA solves single point failure of coordinators. Users can submit queries to any coordinator to balance the workloads.

## Installing HA
openLooKeng with HA is required to be installed with minimum of 2 coordinators in the cluster. Make sure the time on all coordinators have been sync up.
Please follow [Deploying openLooKeng Manually](./deployment.md) or [Deploying openLooKeng Automatically](./deployment-auto.md) for basic installation.

## Configuring HA
- There are 2 mechanisms of configuring HA
    1. TCP-IP : Coordinators discover each other based on their seeds (ie. coordinator's IP address).
    2. Multicast : Coordinators discover each other under the same network.

- Multicast mechanism is not recommended for production since UDP is often blocked in production environments.

### Configuring HA with TCP-IP

#### State Store Properties

Create an `etc\state-store.properties` property file inside both coordinators and workers installation directories.

State store is used to store states that are shared between coordinators and workers.

``` properties
state-store.type=hazelcast
state-store.name=query
state-store.cluster=cluster1

hazelcast.discovery.mode=tcp-ip   
hazelcast.discovery.port=5701       #optional, the default port is 5701
```

The above properties are described below:
- `state-store.type`: The type of the state store. For now, only support hazelcast.
- `state-store.name`: User defined name of state store.
- `state-store.cluster`: User defined cluster name of state store.
- `hazelcast.discovery.mode` : The discovery mode of hazelcast state store, now support tcp-ip and multicast(default).  
- `hazelcast.discovery.port` : The user defined port of hazelcast state store to launch. 

#### Seed Store Properties
Create an `etc\seed-store.properties` property file inside both coordinators and workers installation directories.

Seed store is used to store seeds of coordinators to discover each other.

```
seed-store.type=filebased
seed-store.filesystem.seed-dir=/tmp/openlookeng/ha
seed-store.filesystem.profile=hdfs-config-default
```

The above properties are described below:

- `seed-store.type`: The type of the seed store. For now, only support filebased.
- `seed-store.filesystem.seed-dir`: The directory to store seeds.
- `seed-store.filesystem.profile.`: The name of filesystem configuration file under `etc\filesystem` directory.

#### Filesystem Properties

Create an `etc\filesystem\hdfs-config-default.properties` property file inside both coordinators and workers installation directories.

Filesystem is required to be distributed filesystem so that all coordinators and workers can access  (ie. HDFS).
```
fs.client.type=hdfs
hdfs.config.resources=/path/to/core-site.xml,/path/to/hdfs-site.xml
hdfs.authentication.type=NONE
fs.hdfs.impl.disable.cache=true
```
The above properties are described in [Filesystem Access Utilities](../develop/filesystem.md).

#### Coordinator and Worker Properties

Add following configuration to `etc/config.properties` on all coordinators.

``` properties
hetu.multiple-coordinator.enabled=true
hetu.embedded-state-store.enabled=true
hetu.seed-store.enabled=true
```

Add following configuration to ``etc/config.properties`` on all workers.

``` properties
hetu.multiple-coordinator.enabled=true
hetu.seed-store.enabled=true
```

The above properties are described below:

-  `hetu.multiple-coordinator.enabled`: Multiple coordinator mode is enabled.
-  `hetu.embedded-state-store.enabled`: Allow coordinators to use embedded state store.
-  `hetu.seed-store.enabled`: Allow coordinator and worker to use seed store.

### Configuring HA with Multicast

#### State Store Properties

Create an `etc\state-store.properties` file inside both coordinators and workers installation directories.
``` properties
state-store.type=hazelcast
state-store.name=query
state-store.cluster=cluster1

hazelcast.discovery.mode=multicast  #optional, the default is multicast 
hazelcast.discovery.port=5701       #optional, the default port is 5701
```

#### Coordinator and Worker Properties

Add following configuration to `etc/config.properties` on all coordinators.

``` properties
hetu.multiple-coordinator.enabled=true
hetu.embedded-state-store.enabled=true
```

Add following configuration to ``etc/config.properties`` on all workers.

``` properties
hetu.multiple-coordinator.enabled=true
```

## HA Cluster behind Reverse Proxy

To achieve the full benefit of HA, clients(ie. openLooKeng CLI, JDBC, etc) are encouraged to _not_ connect to specific coordinators directly. Instead, they should connect to multiple coordinators through some sort of _reverse proxy_, for example through a load balancer, or a Kubernetes service. This allows the client to continue to work even if a specific coordinator is not working as expected.

### Reverse Proxy Requirement

When the connection is made through a reverse proxy, it is required for a given client to connect to the same coordinator during the execution of a query. This is to ensure a constant heartbeat between the client and coordinator while that query is running. This can be achieved by enabling _sticky_ connections, for example, Nginx's `ip_hash`.

### Configure Reverse Proxy Example (Nginx)

Please include configuration below in the Nginx configuration file (ie. `nginx.conf`).

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