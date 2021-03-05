# 水平伸缩

## 用例

### 自动/手动伸缩

openLooKeng集群可动态增减节点，支持伸缩场景。虽然*资源提供者*有责任确定何时进行伸缩以及在进行缩容时要删除哪些节点，但openLooKeng会确保这些更改正常生效。特别是，在进行缩容期间，删除节点*不*会影响该节点上的工作负载。

对于自动伸缩场景，资源提供者可能希望基于以下因素做出决策：

- 系统指标，如CPU、内存、I/O，或
- 节点工作负载状态（参见下面的状态API）

例如，openLooKeng可以部署在Kubernetes环境中，使用`HorizontalPodAutoscaler`自动伸缩集群大小。你可以在`hetu-samples`模块的`Kubernetes`下找到一些示例部署数据。

### 节点维护

如果一个节点需要暂时从openLooKeng集群中分离出来，例如执行一些维护任务，可以先*隔离*该节点，然后再重新加入集群。

## 节点状态API

通过调用节点的状态API，可以获取该节点的工作负载信息。包括CPU和内存的分配和使用信息。返回值结构请参考`io.prestosql.server.NodeStatus`类。

例如，使用`curl`并假设节点地址为`1.2.3.4:8080`。

```sh
$ curl http://1.2.3.4:8080/v1/status | jq

{
  "nodeId": "...",
  ...
  "memoryInfo": {
    "availableProcessors": ...,
    "totalNodeMemory": "...",
    ...
  },
  "processors": ...,
  "processCpuLoad": ...,
  "systemCpuLoad": ...,
  "heapUsed": ...,
  "heapAvailable": ...,
  "nonHeapUsed": ...
}

```

## 节点状态管理接口

可通过节点状态管理接口实现伸缩和隔离。

### 关闭节点

如果集群不再需要一个节点（例如：作为缩容操作的一部分），可以关闭该节点。

这样的过程是*优雅的*，因为

- 集群不会向该节点分配新的工作负载，并且
- 节点在关闭主进程之前会尝试完成所有现有的工作负载。

关闭过程*不可逆*。可以通过REST API将`SHUTTING_DOWN`状态放到节点的`info/state`端点来启动该过程。例如：

```sh
$ curl -X PUT -H "Content-Type: application/json" http://1.2.3.4:8080/v1/info/state \
    -d '"SHUTTING_DOWN"'
```

### 隔离节点

隔离是将节点暂时从集群中移除。

- 隔离可以像关闭一样优雅：首先等待工作负载完成。处于等待状态的节点处于`isolating`状态。
- 隔离也可以以不优雅的方式进行，直接进入`isolated`状态。
- `Isolating`和`isolated`的节点不再被分配新的工作负载。

通过REST API修改节点的隔离状态，不同的隔离状态对应不同的目标状态。例如：

```sh
# To gracefully isolate a node
$ curl -X PUT -H "Content-Type: application/json" http://1.2.3.4:8080/v1/info/state \
    -d '"ISOLATING"'

# To isolate a node immediately
$ curl -X PUT -H "Content-Type: application/json" http://1.2.3.4:8080/v1/info/state \
    -d '"ISOLATED"'

# To make the node available again
$ curl -X PUT -H "Content-Type: application/json" http://1.2.3.4:8080/v1/info/state \
    -d '"ACTIVE"'
```

### 节点状态迁移

openLooKeng节点（协调节点或工作节点）有五种状态：

- 不活动
- 活动
- 正在隔离
- 已隔离
- 正在关闭

关闭和隔离操作使节点在这些状态之间迁移：

![node-state-transitions](../images/node-state-transitions.png)

1. 除`INACTIVE`外，允许到当前状态的迁移，但是没有效果
1. 请求隔离节点，不分配新的工作负载，等待已有工作负载完成
1. 通过不分配新的工作负载来立即隔离节点。如有现有的工作负载，则被视为不重要，可以不完成
1. 当节点处于`ISOLATING`状态且活动工作负载完成时自动转换
1. 恢复节点正常运行
1. 请求关闭节点，不分配新的工作负载，等待已有工作负载完成
1. 当节点处于`SHUTTING_DOWN`状态且活动工作负载完成时自动转换