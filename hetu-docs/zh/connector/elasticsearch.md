+++
weight = 3
title = "Elasticsearch"
+++

# Elasticsearch连接器


## 概述

Elasticsearch连接器允许从openLooKeng访问Elasticsearch数据。本文档主要介绍如何搭建Elasticsearch连接器来对Elasticsearch执行SQL查询。

**注意**：*强烈推荐使用Elasticsearch 6.0.0及以上版本。*

配置

要配置Elasticsearch连接器，请创建具有以下内容的目录属性文件`etc/catalog/elasticsearch.properties`，并适当替换以下属性：

```{.none}
connector.name=elasticsearch
elasticsearch.default-schema-name=default
elasticsearch.table-description-directory=etc/elasticsearch/
elasticsearch.scroll-size=1000
elasticsearch.scroll-timeout=1m
elasticsearch.request-timeout=2s
elasticsearch.max-request-retries=5
elasticsearch.max-request-retry-time=10s
```

## 配置属性

配置属性包括：

| 属性名称| 说明|
|:----------|:----------|
| `elasticsearch.default-schema-name`| 表的默认模式名。|
| `elasticsearch.table-description-directory`| 包含JSON表描述文件的目录。|
| `elasticsearch.scroll-size`| 每次Elasticsearch滚动请求返回的最大命中数。|
| `elasticsearch.scroll-timeout`| 为滚动请求保持搜索上下文活动的超时。|
| `elasticsearch.max-hits`| 单个Elasticsearch请求最大取回命中次数。|
| `elasticsearch.request-timeout`| Elasticsearch请求超时。|
| `elasticsearch.max-request-retries`| Elasticsearch请求重试最大次数。|
| `elasticsearch.max-request-retry-time`| 当重试失败的请求的时候，使用指数退避，从1秒开始，最多退避到此配置指定的值。|

### `elasticsearch.default-schema-name`

定义将包含没有限定模式名称的所有表的模式。

此属性是可选的；默认值为`default`。

### `elasticsearch.table-description-directory`

指定openLooKeng部署目录下的一个路径，该路径包含一个或多个带有表描述（必须以`.json`结尾）的JSON文件。

此属性是可选的；默认值为`etc/elasticsearch`。

### `elasticsearch.scroll-size`

此属性定义每个Elasticsearch滚动请求中可以返回的最大命中数。

此属性是可选的；默认值为`1000`。

### `elasticsearch.scroll-timeout`

此属性定义了Elasticsearch将保持滚动请求[搜索上下文活动](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html#scroll-search-context )的时间量（毫秒）。

此属性是可选的；默认值为`1s`。

### `elasticsearch.max-hits`

此属性定义Elasticsearch请求可以获取的最大[命中](https://www.elastic.co/guide/en/elasticsearch/reference/current/search.html )次数。

此属性是可选的；默认值为`1000`。

### `elasticsearch.request-timeout`

此属性定义所有Elasticsearch请求的超时值。

此属性是可选的；默认值为`100ms`。

### `elasticsearch.max-request-retries`

定义Elasticsearch请求重试的最大次数。

此属性是可选的；默认值为`5`。

### `elasticsearch.max-request-retry-time`

当重试失败的请求的时候，使用指数退避，从1秒开始，最多退避到此配置指定的值。

此属性是可选的；默认值为`10s`。

## Search Guard身份验证

Elasticsearch连接器提供了额外的安全选项来支持配置为使用Search Guard的Elasticsearch集群。

证书格式可以通过Elasticsearch目录属性文件中的`searchguard.ssl.certificate_format`配置属性进行配置。该配置允许的值为：

| 属性值| 说明|
|:----------|:----------|
| `NONE`（默认）| 不使用Search Guard身份验证。|
| `PEM`| 使用X.509 PEM证书和PKCS #8密钥。|
| `JKS`| 使用keystore和truststore文件。|

如果使用X.509PEM证书和PKCS#8密钥，必须设置以下属性：

| 属性名称| 说明|
|:----------|:----------|
| `searchguard.ssl.pemcert-filepath`| X.509节点证书链路径。|
| `searchguard.ssl.pemkey-filepath`| 证书密钥文件路径。|
| `searchguard.ssl.pemkey-password`| 密钥密码。如果密钥没有密码，则忽略此设置。|
| `searchguard.ssl.pemtrustedcas-filepath`| 根CA路径（PEM格式）。|

如果使用keystore和truststore文件，需要设置如下属性：

| 属性名称| 说明|
|:----------|:----------|
| `searchguard.ssl.keystore-filepath`| keystore文件的路径。|
| `searchguard.ssl.keystore-password`| keystore密码。|
| `searchguard.ssl.truststore-filepath`| truststore文件的路径。|
| `searchguard.ssl.truststore-password`| truststore密码。|

### `searchguard.ssl.pemcert-filepath`

X.509节点证书链路径。该文件必须可由运行openLooKeng的操作系统用户读取。

此属性是可选的；默认值为`etc/elasticsearch/esnode.pem`。

### `searchguard.ssl.pemkey-filepath`

证书密钥文件路径。该文件必须可由运行openLooKeng的操作系统用户读取。

此属性是可选的；默认值为`etc/elasticsearch/esnode-key.pem`。

### `searchguard.ssl.pemkey-password`

`searchguard.ssl.pemkey-filepath`指定的密钥文件的密钥密码。

此属性是可选的；默认值为空字符串。

### `searchguard.ssl.pemtrustedcas-filepath`

根CA路径（PEM格式）。该文件必须可由运行openLooKeng的操作系统用户读取。

此属性是可选的；默认值为`etc/elasticsearch/root-ca.pem`。

### `searchguard.ssl.keystore-filepath`

keystore文件的路径。该文件必须可由运行openLooKeng的操作系统用户读取。

此属性是可选的；默认值为`etc/elasticsearch/keystore.jks`。

### `searchguard.ssl.keystore-password`

`searchguard.ssl.keystore-filepath`指定的keystore文件的密钥密码。

此属性是可选的；默认值为空字符串。

### `searchguard.ssl.truststore-filepath`

truststore文件的路径。该文件必须可由运行openLooKeng的操作系统用户读取。

此属性是可选的；默认值为`etc/elasticsearch/truststore.jks`。

### `searchguard.ssl.truststore-password`

`searchguard.ssl.truststore-password`指定的truststore文件的密钥密码。

此属性是可选的；默认值为空字符串。

## 表定义文件

Elasticsearch将数据存储在多个节点中，并构建索引以进行快速检索。对于openLooKeng，必须将这些数据映射到列中，以便允许对数据进行查询。

表定义文件以JSON格式描述一个表。

```{.none}
{
    "tableName": ...,
    "schemaName": ...,
    "hostAddress": ...,
    "port": ...,
    "clusterName": ...,
    "index": ...,
    "indexExactMatch": ...,
    "type": ...
    "columns": [
        {
            "name": ...,
            "type": ...,
            "jsonPath": ...,
            "jsonType": ...,
            "ordinalPosition": ...
        }
    ]
}
```

| 字段| 是否必填| 类型| 说明|
|:----------|:----------|:----------|:----------|
| `tableName`| 必填| string| 表名称。|
| `schemaName`| 可选| string| 包含该表的模式。如果省略，则使用默认模式名称。|
| `host`| 必填| string| Elasticsearch搜索节点主机名。|
| `port`| 必填| integer| Elasticsearch搜索节点端口号。|
| `clusterName`| 必填| string| Elasticsearch集群名称。|
| `index`| 必填| string| 正在备份此表的Elasticsearch索引。|
| `indexExactMatch`| 可选| boolean| 如果设置为true，则使用`index`属性指定的索引。否则，将使用`index`属性指定的前缀开始的所有索引。|
| `type`| 必填| string| Elasticsearch[映射类型](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html#mapping-type )，决定文档的索引方式。|
| `columns`| 可选| list| 列元数据信息列表。|

## Elasticsearch列元数据

可选，列元数据可以与以下字段在同一个表描述JSON文件中描述：

| 字段| 是否必填| 类型| 说明|
|:----------|:----------|:----------|:----------|
| `name`| 可选| string| Elasticsearch字段的列名。|
| `type`| 可选| string| Elasticsearch字段的列类型。|
| `jsonPath`| 可选| string| Elasticsearch字段的JSON路径。|
| `jsonType`| 可选| string| Elasticsearch字段的JSON类型。|
| `ordinalPosition`| 可选| integer| 列的序数位置。|

