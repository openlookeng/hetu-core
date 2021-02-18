
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

配置连接Elasticsearch的认证类型。目前仅支持`PASSWORD`。

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

示例：

``` properties
elasticsearch.tls.enabled=true
elasticsearch.tls.keystore-path=/etc/elasticsearch/openLooKeng.jks
elasticsearch.tls.keystore-password=keystore_password
```

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
| `date`| TIMESTAMP|
| `ip`| IPADDRESS|
| `(all others)`| (unsupported)|

Array Type
--------
Elasticsearch中的字段可以包含单个或多个值，但没有数组类型。要说明字段中包含数组，可以在索引映射的_meta中用Presto特定的结构对其进行注释。

例如，有一个包含以下结构的文档的索引:
### 
    {
         "array_string_field": ["presto","is","the","besto"],
         "long_field": 314159265359,
         "id_field": "564e6982-88ee-4498-aa98-df9e3f6b6109",
         "timestamp_field": "1987-09-17T06:22:48.000Z",
         "object_field": {
             "array_int_field": [86,75,309],
             "int_field": 2
         }
     }

可以使用以下命令定义该结构的数组字段，将字段属性定义添加到目标索引映射的_meta.presto属性中。
### 
    curl --request PUT \
        --url localhost:9200/doc/_mapping \
        --header 'content-type: application/json' \
        --data '
    {
        "_meta": {
            "presto":{
                "array_string_field":{
                    "isArray":true
                },
                "object_field":{
                    "array_int_field":{
                        "isArray":true
                    }
                },
            }
        }
    }'

## 限制
1. openLooKeng不支持查询Elasticsticsearch中有重复列名的表，如：列名为“name”和“NAME”；
2. openLooKeng不支持Elasticsticsearch表名含有特殊字符，如‘-’，‘.’等。
