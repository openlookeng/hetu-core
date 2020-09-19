# 审计日志

openLooKeng审计日志记录功能是一个自定义事件监听器，在查询创建和完成（成功或失败）时调用。审计日志包含以下信息：

1. 事件发生时间
2. 用户ID
3. 访问发起方地址或标识
4. 事件类型（操作）
5. 访问资源名称
6. 事件结果

在openLooKeng集群中，一次只能有一个事件侦听器插件处于活动状态。

## 实现

审计日志记录是`HetuListener`插件中`io.prestosql.spi.eventlistener.EventListener`的一个实现。覆盖的方法包括`AuditEventLogger#onQueryCreatedEvent`和`AuditEventLogger#onQueryCompletedEvent`。

## 配置

要启用审计日志记录功能，`etc/event-listener.properties`中必须存在以下配置来激活此功能。

```
hetu.event.listener.type=AUDIT
hetu.event.listener.listen.query.creation=true
hetu.event.listener.listen.query.completion=true
```

其他审计日志记录属性包括：

`hetu.event.listener.audit.file`：可选属性，用于定义审计文件的绝对文件路径。确保运行openLooKeng服务器的进程对该目录有写权限。

`hetu.event.listener.audit.filecount`：可选属性，用于定义要使用的文件数。

`hetu.event.listener.audit.limit`：可选属性，用于定义写入任一文件的最大字节数。

配置文件示例：

```properties
event-listener.name=hetu-listener
hetu.event.listener.type=AUDIT
hetu.event.listener.listen.query.creation=true
hetu.event.listener.listen.query.completion=true
hetu.event.listener.audit.file=/var/log/hetu/hetu-audit.log
hetu.event.listener.audit.filecount=1
hetu.event.listener.audit.limit=100000
```