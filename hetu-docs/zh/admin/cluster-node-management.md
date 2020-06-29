+++

weight = 9
title = "集群节点管理"
+++


# 集群节点管理

openLooKeng集群中可能有多个节点（协调节点和工作节点）。出于扩容或维护目的，管理员操作有时需要在这些节点上执行。

## 关闭节点

如果集群不再需要一个节点（例如：作为缩容操作的一部分），可以关闭该节点。

这样的过程是*优雅的*，因为

- 集群不会向该节点分配新的工作负载，并且
- 节点在关闭主进程之前会尝试完成所有现有的工作负载。

关闭过程*不可逆*。可以通过REST API将`SHUTTING_DOWN`状态放到节点的`info/state`端点来启动该过程。例如，使用`curl`且假设待关闭节点的地址为`1.2.3.4:8080`：

```sh
curl -X PUT -H "Content-Type: application/json" http://1.2.3.4:8080/v1/info/state \
    -d '"SHUTTING_DOWN"'
```

## 隔离节点

隔离是将节点暂时从集群中移除，例如对其执行维护任务。之后可将节点重新加入集群。

- 隔离可以像关闭一样优雅：首先等待工作负载完成。处于等待状态的节点处于`isolating`状态。
- 隔离也可以以不优雅的方式进行，直接进入`isolated`状态。
- `Isolating`和`isolated`的节点不再被分配新的工作负载。

通过REST API修改节点的隔离状态，不同的隔离状态对应不同的目标状态。例如：

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

## 节点状态迁移

openLooKeng节点（协调节点或工作节点）有五种状态：

- 不活动
- 活动
- 正在隔离
- 已隔离
- 正在关闭

关闭和隔离操作使节点在这些状态之间迁移。下表展示哪些迁移是允许的，其中“-”表示允许该迁移，但是没有效果。

| 从\\到| 不活动| 活动| 正在隔离| 已隔离| 关闭中| 退出|
|----------|----------|----------|----------|----------|----------|----------|
| 不活动| | | | | 5| |
| 活动| | \-| 2| 3| 5| |
| 正在隔离| | 1| \-| 3、4| 5| |
| 已隔离| | 1| 2| \-| 5| |
| 关闭中| | | | | \-| 6|

迁移说明：

1. 恢复节点正常运行
2. 请求隔离节点，不分配新的工作负载，等待已有工作负载完成
3. 通过不分配新的工作负载来立即隔离节点。如有现有的工作负载，则被视为不重要，可以不完成
4. 当节点处于`ISOLATING`状态且活动工作负载完成时自动转换
5. 请求关闭节点，不分配新的工作负载，等待已有工作负载完成
6. 当节点处于`SHUTTING_DOWN`状态且活动工作负载完成时自动转换