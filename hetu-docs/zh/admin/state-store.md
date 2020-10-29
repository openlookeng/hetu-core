# 状态存储
本节介绍openLooKeng状态存储. 状态存储用于存储状态,状态被状态存储成员和状态存储客户端共享。

状态存储集群由状态存储成员组成，状态存储客户端可以执行所有状态存储操作而无需成为集群的成员。

强烈建议将协调节点配置为状态存储成员，将工作节点配置为状态存储客户端。

## 使用
当前状态存储被用于HA和动态过滤功能特性。

## 配置状态存储

### 配置状态存储成员
- 添加 `hetu.embedded-state-store.enabled=true` 到 `etc/config.properties`。
- 配置状态存储属性, 参考下方`配置状态存储属性`。 
### 配置状态存储客户端
- 配置状态存储属性, 参考下方`配置状态存储属性`。

### 配置状态存储属性
- 目前有2种方法配置状态存储属性
    1. TCP-IP : 状态存储成员根据种子（即状态存储成员ip地址）相互发现。
    2. Multicast : 状态存储成员在同一网络下相互发现。
- 不建议在生产环境中使用Multicast机制，因为UDP通常在生产环境中被阻止。

### TCP-IP方法配置状态存储属性

在状态存储成员和态存储客户端安装目录中创建文件`etc\state-store.properties`。

``` properties
state-store.type=hazelcast
state-store.name=query
state-store.cluster=cluster1

hazelcast.discovery.mode=tcp-ip   
hazelcast.discovery.port=5701       
#在hazelcast.discovery.tcp-ip.seeds或者hazelcast.discovery.tcp-ip.profile配置任选其一
hazelcast.discovery.tcp-ip.seeds=<member1_ip:member1_hazelcast.discovery.port>,<member2_ip:member2_hazelcast.discovery.port>,...
hazelcast.discovery.tcp-ip.profile=hdfs-config-default
```

上述属性说明如下：

- `state-store.type`：状态存储的类型。目前仅支持Hazelcast。
- `state-store.name`：用户定义的状态存储名称。
- `state-store.cluster`：用户定义的状态存储集群名称。
- `hazelcast.discovery.mode` : Hazelcast状态存储发现模式, 目前支持tcp-ip和multicast（默认）。 
- `hazelcast.discovery.port` : 用户定义hazelcast启动端口。该属性可选，默认端口为5701。只需状态存储成员配置该项。
- `hazelcast.discovery.tcp-ip.seeds` : 状态存储成员种子列表用来启动状态存储集群。
- `hazelcast.discovery.tcp-ip.profile` : 共享存储配置文件的名称，可被状态存储成员和状态存储客户端共同访问。目前只支持`文件系统`配置文件, 参考下方`文件系统属性`。

说明：如果状态存储成员的ip地址是静态的，请配置“ hazelcast.discovery.tcp-ip.seeds”。
如果状态存储成员的ip地址是动态变化的，则用户可以配置“ hazelcast.discovery.tcp-ip.profile”，其中状态存储成员会自动将其ip：port存储在共享存储中，发现彼此。
如果两者同时被配置，“ hazelcast.discovery.tcp-ip.seeds”将会被使用。

### 文件系统属性

在状态存储成员和状态存储客户端安装目录中创建文件`etc\filesystem\hdfs-config-default.properties`。

文件系统必须是分布式文件系统，以便所有状态存储成员和状态存储客户端都可以访问（例如HDFS）。
```
fs.client.type=hdfs
hdfs.config.resources=/path/to/core-site.xml,/path/to/hdfs-site.xml
hdfs.authentication.type=NONE
fs.hdfs.impl.disable.cache=true
```
上述属性说明请参考[文件系统访问实用程序](../develop/filesystem.md)。 

## Multicast方法配置状态存储属性

在状态存储成员和状态存储客户端安装目录中创建文件`etc\state-store.properties`。

``` properties
state-store.type=hazelcast
state-store.name=query
state-store.cluster=cluster1

hazelcast.discovery.mode=multicast
hazelcast.discovery.port=5701       
```