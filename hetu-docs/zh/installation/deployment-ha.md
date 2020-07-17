
# 部署具有高可用（HA）的openLooKeng

openLooKeng HA解决协调节点单点故障问题。用户可以在任何协调节点上提交查询，以平衡工作负载。

## 安装HA

安装具有HA的openLooKeng要求集群中至少有2个协调节点。请按照[手动部署openLooKeng](deployment.md)或[自动部署openLooKeng](deployment-auto.md)进行基本设置。

## 配置HA

- 在协调节点和工作节点安装目录中创建文件`etc\state-store.properties`。

### 状态存储属性

状态存储属性文件`etc/state-store.properties`包含状态存储的配置。状态存储用于与协调节点和工作节点共享的存储状态。以下是最基本配置。

``` properties
state-store.type=hazelcast
state-store.name=query
state-store.cluster=cluster1
```

上述属性说明如下：

- `state-store.type`：状态存储的类型。目前仅支持Hazelcast。
- `state-store.name`：用户定义的状态存储名称。
- `state-store.cluster`：用户定义的状态存储集群名称。

### 协调节点和工作节点属性

在所有协调节点上的`etc/config.properties`中添加如下配置。

``` properties
hetu.multiple-coordinator.enabled=true
hetu.embedded-state-store.enabled=true
```

在所有工作节点上的`etc/config.properties`中添加如下配置。

``` properties
hetu.multiple-coordinator.enabled=true
```

上述属性说明如下：

- `hetu.multiple-coordinator.enabled`：开启了多协调节点模式。
- `hetu.embedded-state-store.enabled`：允许此协调节点使用嵌入式状态存储。