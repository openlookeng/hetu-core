
# Deploying openLooKeng with High Availability(HA)

The openLooKeng HA solves single point failure of coordinators. Users can submit queries to any coordinator to balance the workloads.

## Installing HA
openLooKeng with HA is required to be installed with minimum of 2 coordinators in the cluster. Make sure the time on all coordinators have been sync up.
Please follow [Deploying openLooKeng Manually](./deployment.md) or [Deploying openLooKeng Automatically](./deployment-auto.md) for basic installation.

## Configuring HA

###Configuring Coordinator and Worker
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
- `hetu.multiple-coordinator.enabled`: Enable multiple coordinators.
- `hetu.embedded-state-store.enabled`: Enable coordinators to start embedded state store. 

Note: It is suggested to enable embedded state store on all coordinators(or at least 3) to guarantee the high availability of service when node/network is down.

###Configuring State Store
Please refer to the section [State Store](../admin/state-store.md) to configure state store.

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