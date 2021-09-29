# State Store
This section describes the openLooKeng state store. State store is used to store states that are shared between state store members and state store clients.

State store cluster is composed of state store members, and state store clients can do all state store operations without being a member of cluster.

It is highly recommended that configure coordinators as state store members and workers as state store clients.

## Usage
State store is currently used by HA and dynamic filters features.

## Configuring State Store

### Configuring State Store Members
- Add `hetu.embedded-state-store.enabled=true` in `etc/config.properties`.
- Configure state store properties, described in the section `Configuring State Store Properties`.    

### Configuring State Store Clients
- Configure state store properties, described in the section `Configuring State Store Properties`.    

### Configuring State Store Properties
- There are 2 mechanisms of configuring state store properties.
    1. TCP-IP : State store members discover each other based on their seeds (ie. member's IP address).
    2. Multicast : State store members discover each other under the same network.

- Multicast mechanism is not recommended for production since UDP is often blocked in production environments.

#### Configuring State Store Properties with TCP-IP

Create an `etc\state-store.properties` property file inside both state store members and clients installation directories.

``` properties
state-store.type=hazelcast
state-store.name=query
state-store.cluster=cluster1

hazelcast.discovery.mode=tcp-ip
hazelcast.discovery.port=5701
#configure either hazelcast.discovery.tcp-ip.seeds OR hazelcast.discovery.tcp-ip.profile
hazelcast.discovery.tcp-ip.seeds=<member1_ip:member1_hazelcast.discovery.port>,<member2_ip:member2_hazelcast.discovery.port>,...
hazelcast.discovery.tcp-ip.profile=hdfs-config-default
```

The above properties are described below:
- `state-store.type`: The type of the state store. For now, only support hazelcast.
- `state-store.name`: User defined name of state store.
- `state-store.cluster`: User defined cluster name of state store.
- `hazelcast.discovery.mode` : The discovery mode of hazelcast state store, now support tcp-ip and multicast(default).  
- `hazelcast.discovery.port` : The user defined port of hazelcast state store to launch. This property is optional and the default is 5701. Only state store members need to configure this.
- `hazelcast.discovery.tcp-ip.seeds` : A list of state store members' seeds used to form state store cluster.
- `hazelcast.discovery.tcp-ip.profile` : The name of configuration file that represents a shared storage accessed by state store members and clients. Now it only supports `Filesystem` profile, described in the section `Filesystem Properties`.

Note: Please configure `hazelcast.discovery.tcp-ip.seeds` if state store members' ips are static. 
If state store members' ips are dynamic, user can configure `hazelcast.discovery.tcp-ip.profile` where members will store its ip:port in the profile, and discover each other automatically. 
If both are configured, `hazelcast.discovery.tcp-ip.seeds` will be used.

#### Filesystem Properties

Create an `etc\filesystem\hdfs-config-default.properties` property file inside both state store members and clients installation directories.

Filesystem is required to be distributed filesystem so that all state store members and clients can access (ie. HDFS).
```
fs.client.type=hdfs
hdfs.config.resources=<hdfs_config_dir>/core-site.xml,<hdfs_config_dir>/hdfs-site.xml
hdfs.authentication.type=NONE
fs.hdfs.impl.disable.cache=true
```
The above properties are described in [Filesystem Access Utilities](../develop/filesystem.md).

### Configuring State Store Properties with Multicast

Create an `etc\state-store.properties` file inside both state store members and clients installation directories.
``` properties
state-store.type=hazelcast
state-store.name=query
state-store.cluster=cluster1

hazelcast.discovery.mode=multicast 
hazelcast.discovery.port=5701       
```
The above properties are described the section `Configuring State Store Properties with TCP-IP`.

## Configuring State Store Example
This example describes how to configure state store cluster of 2 coordinators + 2 workers using `TCP-IP` discovery mode. Coordinators are configured as state store members and workers are configured as state store client.

## Prerequisite and Assumption
- coordinator1'ip=10.100.100.01, coordinator2's ip=10.100.100.02.
- worker1'ip=10.100.100.03, worker2'ip=10.100.100.04.
- HDFS is installed, core-site.xml and hdfs-site.xml are located in `/opt/hdfs` directory.

#### Configuring Steps
- Coordinators configuring steps
   1. Open `etc/config.properties` and add following properties.
      ```
      hetu.embedded-state-store.enabled=true
      ```
   2. Create `etc\state-store.properties` property file and add following properties.
      ```
       state-store.type=hazelcast
       state-store.name=query
       state-store.cluster=cluster1
      
       hazelcast.discovery.mode=tcp-ip
       hazelcast.discovery.port=5701
       hazelcast.discovery.tcp-ip.seeds=10.100.100.01:5701,10.100.100.02:5701
       hazelcast.discovery.tcp-ip.profile=hdfs-config-default
      ```
   3. Create an `etc\filesystem\hdfs-config-default.properties` property file and add following properties.
      ```
      fs.client.type=hdfs
      hdfs.config.resources=/opt/hdfs/core-site.xml,/opt/hdfs/hdfs-site.xml
      hdfs.authentication.type=NONE
      fs.hdfs.impl.disable.cache=true
      ```
- Workers configuring steps
   1. Create `etc\state-store.properties` property file and add following properties.
      ```
       state-store.type=hazelcast
       state-store.name=query
       state-store.cluster=cluster1
      
       hazelcast.discovery.mode=tcp-ip
       hazelcast.discovery.tcp-ip.seeds=10.100.100.01:5701,10.100.100.02:5701
       hazelcast.discovery.tcp-ip.profile=hdfs-config-default
      ```
   2. Create an `etc\filesystem\hdfs-config-default.properties` property file and add following properties.
      ```
      fs.client.type=hdfs
      #Assume core-site.xml and hdfs-site.xml are located in /opt/hdfs directory
      hdfs.config.resources=/opt/hdfs/core-site.xml,/opt/hdfs/hdfs-site.xml
      hdfs.authentication.type=NONE
      fs.hdfs.impl.disable.cache=true
      ```

