

# 事件侦听器

openLooKeng 支持为以下事件调用的自定义事件侦听器：


-   创建查询
-   完成查询（成功或失败）
-   完成分片（成功或失败）

利用该功能，可以开发自定义日志记录、调试和性能分析插件。在 openLooKeng 集群中，同一时间只有一个事件侦听器插件处于活动状态。

## 实现

`EventListenerFactory` 负责创建 `EventListener` 实例。它还定义管理员在 openLooKeng 配置中使用的 `EventListener` 名称。`EventListener` 实现可以实现用于其需要处理的事件类型的方法。


必须将 `EventListener` 和 `EventListenerFactory` 的实现包装为一个插件并将其安装在 openLooKeng 集群上。

## 配置

在协调节点上安装实现 `EventListener` 和 `EventListenerFactory` 的插件后，使用 `etc/event-listener.properties` 文件对其进行配置。除 `event-listener.name` 之外，所有属性都特定于 `EventListener` 实现。

`Event-listener.name` 属性由 openLooKeng 用于根据 `EventListenerFactory.getName()` 返回的名称查找注册的 `EventListenerFactory`。其余的属性作为到 `EventListenerFactory.create()` 的映射进行传递。

配置文件示例：

``` properties
event-listener.name=custom-event-listener
custom-property1=custom-value1
custom-property2=custom-value2
```