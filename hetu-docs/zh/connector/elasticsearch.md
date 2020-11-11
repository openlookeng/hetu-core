
# Elasticsearch连接器


## 概述

Elasticsearch连接器允许从openLooKeng访问Elasticsearch数据。本文档主要介绍如何搭建Elasticsearch连接器来对Elasticsearch执行SQL查询。

**注意**：*强烈推荐使用Elasticsearch 6.0.0及以上版本。*

配置

要配置Elasticsearch连接器，请创建具有以下内容的目录属性文件`etc/catalog/elasticsearch.properties`，并适当替换以下属性：

``` properties
connector.name=elasticsearch
elasticsearch.host=localhost
elasticsearch.port=9200
elasticsearch.default-schema-name=default
```

## 配置属性

### `elasticsearch.host`

定义连接Elasticsearch节点的主机。

此属性是必选。

### `elasticsearch.port`

定义连接Elasticsearch节点的端口号。

此属性是可选的；默认值为9200。 

### `elasticsearch.default-schema-name`

定义将包含没有限定模式名称的所有表的模式。

此属性是可选的；默认值为`default`。

### `elasticsearch.scroll-size`

此属性定义每个Elasticsearch滚动请求中可以返回的最大命中数。

此属性是可选的；默认值为`1000`。

### `elasticsearch.scroll-timeout`

此属性定义了Elasticsearch将保持滚动请求的时间量（毫秒）。

此属性是可选的；默认值为`1m`。

### `elasticsearch.request-timeout`

此属性定义所有Elasticsearch请求的超时值。

此属性是可选的；默认值为`10s`。

### `elasticsearch.connect-timeout`

此属性定义了所有连接Elasticsearch的超时值。

此属性是可选的；默认值为`1s`。

### `elasticsearch.max-retry-time`

此属性定义了单个连接的最大重试时间。

此属性是可选的；默认值为`20s`。

### `elasticsearch.node-refresh-interval`

此属性定义了可用Elasticsearch节点更新频率。

此属性是可选的；默认值为`1m`。

### `elasticsearch.security`

连接Elasticsearch启用密码认证。

### `elasticsearch.auth.user`

连接Elasticsearch认证的用户名。 

### `elasticsearch.auth.password`

连接Elasticsearch认证的密码。 

TLS Security
--------

Elasticsearch连接器提供了额外安全选项支持开启了TLS的Elasticsearch节点。该连接器支持PEM或JKS格式的key store和trust store。配置参数如下：

### `elasticsearch.tls.keystore-path`

此属性定义了PEM或JKS格式的key store文件的路径, 该文件必须可由运行openLooKeng的操作系统用户读取。

此属性是可选的。

### `elasticsearch.tls.truststore-path`

此属性定义了PEM或JKS格式的trust store文件的路径, 该文件必须可由运行openLooKeng的操作系统用户读取。

此属性是可选的。

### `elasticsearch.tls.keystore-password`

此属性定义了key store文件的密码。

此属性是可选的。

### `elasticsearch.tls.truststore-password`

此属性定义了trust store文件的密码。

此属性是可选的。

Data Types
--------

数据类型映射关系如下：

| Elasticsearch| openLooKeng|
|:----------|:----------|
| `binary`|  VARBINARY|
| `boolean`| BOOLEAN|
| `double`| DOUBLE|
| `float`| REAL|
| `byte`| TINYINT|
| `short`| SMALLINT|
| `integer`| INTEGER|
| `long`| BIGINT|
| `keyword`| VARCHAR|
| `text`| VARCHAR|
| `(all others)`| (unsupported)|

