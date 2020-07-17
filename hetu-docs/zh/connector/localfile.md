
# 本地文件连接器

本地文件连接器允许查询每个工作节点的本地文件系统上存储的数据。

## 配置

要配置本地文件连接器，需在`etc/catalog`目录下新建一个例如名为`localfile.properties`的文件，内容如下：

``` properties
connector.name=localfile
```

## 配置属性

| 属性名称| 说明|
|:----------|:----------|
| `presto-logs.http-request-log.location`| 写入HTTP请求日志的目录或文件|
| `presto-logs.http-request-log.pattern`| 如果日志位置是一个目录，这个glob将用于匹配目录中的文件名。|

## 本地文件连接器模式和表

本地文件连接器提供了一个名为`logs`的模式。通过执行`SHOW TABLES`，可以看到所有可用的表：

    SHOW TABLES FROM localfile.logs;

### `http_request_log`

该表包含来自集群中每个节点的HTTP请求日志。