
# 部署具有高可用（HA）的openLooKeng

openLooKeng HA解决协调节点单点故障问题。用户可以在任何协调节点上提交查询，以平衡工作负载。

## 安装HA

安装具有HA的openLooKeng要求集群中至少有2个协调节点。请确保协调节点上的时间一致性。请按照[手动部署openLooKeng](deployment.md)或[自动部署openLooKeng](deployment-auto.md)进行基本设置。

## 配置HA
- 目前有2种方法配置HA
    1. TCP-IP : 协调节点根据种子（即协调节点ip地址）相互发现。
    2. Multicast : 协调节点在同一网络下相互发现。
- 不建议在生产环境中使用Multicast机制，因为UDP通常在生产环境中被阻止。

### TCP-IP方法配置HA

#### 状态存储属性
在协调节点和工作节点安装目录中创建文件`etc\state-store.properties`。

状态存储用于与协调节点和工作节点共享的存储状态。

``` properties
state-store.type=hazelcast
state-store.name=query
state-store.cluster=cluster1

hazelcast.discovery.mode=tcp-ip   
hazelcast.discovery.port=5701       #可选，默认端口为5701
```

上述属性说明如下：

- `state-store.type`：状态存储的类型。目前仅支持Hazelcast。
- `state-store.name`：用户定义的状态存储名称。
- `state-store.cluster`：用户定义的状态存储集群名称。
- `hazelcast.discovery.mode` : Hazelcast状态存储发现模式, 目前支持tcp-ip和multicast（默认）。  
- `hazelcast.discovery.port` : 用户定义hazelcast启动端口。

#### 种子存储属性
在协调节点和工作节点安装目录中创建文件`etc\seed-store.properties`。

种子存储用于存储协调节点的种子以相互发现。

```
seed-store.type=filebased
seed-store.filesystem.seed-dir=/tmp/openlookeng/ha
seed-store.filesystem.profile=hdfs-config-default
```

上述属性说明如下：

- `seed-store.type`: 种子存储的类型。目前仅支持Filebased。
- `seed-store.filesystem.seed-dir`: 存储种子的目录。
- `seed-store.filesystem.profile.`: `etc\filesystem`目录下的文件系统中配置文件的名称。


#### 文件系统属性

在协调节点和工作节点安装目录中创建文件`etc\filesystem\hdfs-config-default.properties`。

文件系统必须是分布式文件系统，以便所有协调节点和工作节点都可以访问（例如HDFS）。
```
fs.client.type=hdfs
hdfs.config.resources=/path/to/core-site.xml,/path/to/hdfs-site.xml
hdfs.authentication.type=NONE
fs.hdfs.impl.disable.cache=true
```
上述属性说明请参考[文件系统访问实用程序](../develop/filesystem.md)。 

#### 协调节点和工作节点属性

在所有协调节点上的`etc/config.properties`中添加如下配置。

``` properties
hetu.multiple-coordinator.enabled=true
hetu.embedded-state-store.enabled=true
hetu.seed-store.enabled=true
```

在所有工作节点上的`etc/config.properties`中添加如下配置。

``` properties
hetu.multiple-coordinator.enabled=true
hetu.seed-store.enabled=true
```

上述属性说明如下：

- `hetu.multiple-coordinator.enabled`：开启了多协调节点模式。
- `hetu.embedded-state-store.enabled`：允许协调节点使用嵌入式状态存储。
- `hetu.seed-store.enabled`: 允许协调节点和工作节点使用嵌入式状态存储。

### Multicast方法配置HA

#### 状态存储属性

状态存储属性文件`etc/state-store.properties`包含状态存储的配置。

``` properties
state-store.type=hazelcast
state-store.name=query
state-store.cluster=cluster1
```

#### 协调节点和工作节点属性

在所有协调节点上的`etc/config.properties`中添加如下配置。

``` properties
hetu.multiple-coordinator.enabled=true
hetu.embedded-state-store.enabled=true
```

在所有工作节点上的`etc/config.properties`中添加如下配置。

``` properties
hetu.multiple-coordinator.enabled=true
```

## HA及反向代理

要尽可能的显示HA的优势，建议客户端(例如openLooKeng CLI, JDBC等)不直接连接特定的协调节点，而是通过反向代理连接多个协调节点，例如使用负载平衡器或者Kubernetes服务，这样即使某个协调节点无法正常工作，客户端也可以继续使用其它的协调节点。

### 反向代理要求

通过反向代理连接时，要求客户端在执行语句查询期间连接到同一协调器，确保在语句查询运行时客户端和协调器之间保持恒定心跳。这可以通过配置反向代理实现，例如Nginx的`ip_hash`。

### 配置反向代理 (Nginx)

请在Nginx配置文件中包含以下配置（nginx.conf）。

```
http {
    ...  # 用户自定义配置
    upstream backend {
        ip_hash;
        server <coordinator1_ip>:<coordinator1_port>;
        server <coordinator2_ip>:<coordinator2_port>;
        server <coordinator3_ip>:<coordinator3_port>;
        ...
    }

    server {
        ... # 用户自定义配置
        location / {
            proxy_pass http://backend;
            proxy_set_header Host <nginx_ip>:<nginx_port>;
        }
    }
}
```