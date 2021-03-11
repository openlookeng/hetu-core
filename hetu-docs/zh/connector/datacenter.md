# 数据中心连接器

数据中心连接器允许查询远程openLooKeng数据中心。可实现来自本地openLooKeng环境的不同openLooKeng集群之间的数据融合分析。

## 本端连接器配置

配置数据中心连接器时，在`etc/catalog`中创建一个属性文件，例如`<dc-name>.properties`，即将数据中心连接器挂载到`<dc-name>` 目录。使用以下内容创建配置文件，并根据实际的远端openLooKeng替换`connection`属性：

### 基本配置

```{.none}
connector.name=dc
connection-url=http://example.net:8080
connection-user=<远端openLooKeng用户名>
connection-password=<远端openLooKeng密码>
```

| 属性名称              | 说明                          | 是否必选 | 默认值 |
| --------------------- | ----------------------------- | :------- | ------ |
| `connection-url`      | 需要连接的openLooKeng的URL    | 必选     |        |
| `connection-user`     | 需要连接的openLooKeng的用户名 | 可选     |        |
| `connection-password` | 需要连接的openLooKeng的密码   | 可选     |        |

### 安全配置

当远端openLooKeng开启了安全认证或TLS/SSL通道加密时，则需要在`<dc-name>.properties`进行相应的安全配置。

#### Kerberos认证方式

| 属性名称                                | 说明                                                         | 默认值               |
| --------------------------------------- | ------------------------------------------------------------ | -------------------- |
| `dc.kerberos.config.path`               | Kerberos配置文件                                             |                      |
| `dc.kerberos.credential.cachepath`      | Kerberos凭证缓存                                             |                      |
| `dc.kerberos.keytab.path`               | Kerberos keytab文件                                          |                      |
| `dc.kerberos.principal`                 | 向openLooKeng协调节点进行身份验证时使用的主体                |                      |
| `dc.kerberos.remote.service.name`       | openLooKeng协调节点Kerberos服务的名称。Kerberos身份验证时，需要配置该参数 |                      |
| `dc.kerberos.service.principal.pattern` | openLooKeng协调节点Kerberos服务主体模式                      | `${SERVICE}@${HOST}` |
| `dc.kerberos.use.canonical.hostname`    | 通过首先将主机名解析为IP地址，然后对该IP地址执行反向DNS查找，从而使用Kerberos服务主体的openLooKeng协调节点的规范主机名 | `false`              |

#### 令牌认证方式

| 属性名称         | 说明                       | 默认值 |
| ---------------- | -------------------------- | ------ |
| `dc.accesstoken` | 基于令牌身份验证的访问令牌 |        |

#### 外部证书认证方式
| 属性名称               | 说明                                                         | 默认值 |
| ---------------------- | ------------------------------------------------------------ | ------ |
| `dc.extra.credentials` | 连接外部服务的额外凭证。ExtraCredentials是一个键值对列表。示例：**foo:bar;abc:xyz**将创建凭证**abc=xyz**和**foo=bar**。 |        |

#### SSL/TLS

| 属性名称                     | 说明                                                 | 默认值  |
| ---------------------------- | ---------------------------------------------------- | ------- |
| `dc.ssl`                     | 使用HTTPS连接                                        | `false` |
| `dc.ssl.keystore.password`   | keystore密码                                         |         |
| `dc.ssl.keystore.path`       | 包含用于身份验证的证书和私钥的JavaKeyStore文件的位置 |         |
| `dc.ssl.truststore.password` | truststore密码                                       |         |
| `dc.ssl.truststore.path`     | 用于验证HTTPS服务器证书的Java TrustStore文件的位置   |         |

### 代理配置

| 属性名称        | 说明                                          | 默认值 |
| --------------- | --------------------------------------------- | ------ |
| `dc.socksproxy` | SOCKS代理主机和端口。示例：**localhost:1080** |        |
| `dc.httpproxy`  | HTTP代理主机和端口。示例：**localhost:8888**  |        |

### 性能优化配置

| 属性名称                         | 说明                                                         | 默认值  |
| -------------------------------- | ------------------------------------------------------------ | ------- |
| `dc.metadata.cache.enabled`      | 启用元数据缓存，缓存远端openLooKeng的元数据信息              | `true`  |
| `dc.metadata.cache.maximum.size` | 元数据缓存最大值，可缓存的远端openLooKeng元数据条数          | `10000` |
| `dc.metadata.cache.ttl`          | 元数据缓存TTL，TTL到期，则需要重新从远端openLooKeng获取元数据 | `1.00s` |
| `dc.query.pushdown.enabled`      | 启用子查询下推到远端openLooKeng                              | `true`  |
| `dc.query.pushdown.module`       | FULL_PUSHDOWN，表示全部下推；BASE_PUSHDOWN，表示部分下推，其中部分下推是指filter/aggregation/limit/topN/project这些可以下推。| `FULL_PUSHDOWN`  |
| `dc.http-compression`            | 启用zstd压缩数据                                             | `false` |

### 其他配置

| 属性名称                                        | 说明                                                         | 默认值    |
| ----------------------------------------------- | ------------------------------------------------------------ | --------- |
| `dc.http-request-connectTimeout`                | HTTP请求连接超时，默认值为30秒                               | `30.00s`  |
| `dc.http-request-readTimeout`                   | HTTP请求读取超时，默认为30秒                                 | `30.00s`  |
| `dc.httpclient.maximum.idle.connections`        | HTTP客户端保持打开的最大空闲连接                             | `20`      |
| `dc.http-client-timeout`                        | 客户端持续重试取数据的时间，默认值为10分钟                   | `10.00m`  |
| `dc.max.anticipated.delay`                      | 集群中两个查询请求之间的最大预期时延。如果远程dc没有收到超过此延迟的请求，则可能会取消查询 | `10.00m`  |
| `dc.application.name.prefix`                    | 添加到任何指定的ApplicationName客户端信息属性的前缀，该前缀用于设置openLooKeng查询的源名称。如果没有设置此属性或ApplicationName，则查询的源将是hetu-dc | `hetu-dc` |
| `dc.remote-http-server.max-request-header-size` | 此属性应等效于远程服务器中**http-server.max-request-header-size**的值 |           |
| `dc.remote.cluster.id`                          | 远程集群的唯一ID                                             |           |

## 远端openLooKeng配置

### 远端openLooKeng配置

可以在`etc/config.properties`中设置以下属性：

| 属性名称                            | 说明                                       | 默认值  |
| ----------------------------------- | ------------------------------------------ | ------- |
| `hetu.data.center.split.count`      | 每个查询允许的最大Split个数                | `5`     |
| `hetu.data.center.consumer.timeout` | 执行查询获取到数据后，等待被取走的最大时延 | `10min` |

### 远端Nginx配置

在远端开启HA，同时使用Nginx作为代理时，需要对Nginx的配置进行一定修改：

```nginx
http {
    upstream for_aa {
        ip_hash;
        server 192.168.0.101:8090;   #coordinator-1 的IP和PORT；
        server 192.168.0.102:8090;   #coordinator-2 的IP和PORT；
        check interval=3000 rise=2 fall=5 timeout=1000 type=http;
    }

    upstream for_cross_region {
        hash $hashKey consistent;
        server 192.168.0.101:8090;   #coordinator-1 的IP和PORT；
        server 192.168.0.102:8090;   #coordinator-2 的IP和PORT；
        check interval=3000 rise=2 fall=5 timeout=1000 type=http;
    }
    
    server {
        listen nginx_ip:8888;
        
        location / {
            proxy_pass http://for_aa;
            proxy_redirect off;
            proxy_set_header Host $host:$server_port;
        }
        
        location ^~/v1/dc/(.*)/(.*) {
            set $hashKey $2;
            proxy_redirect off;
            proxy_pass http://for_cross_region;
		    proxy_set_header Host $host:$server_port;
        }
        
        location ^~/v1/dc/statement/(.*)/(.*)/(.*) {
            set $hashKey $3;
            proxy_redirect off;
            proxy_pass http://for_cross_region;
		    proxy_set_header Host $host:$server_port;
        }
    }
}
```

## 多openLooKeng集群

可以根据需要创建任意多的目录，因此，如果有额外的数据中心，只需添加另一个不同的名称的属性文件到`etc/catalog`中（确保它以`.properties`结尾）。例如，如果将属性文件命名为`sales.properties`，openLooKeng将使用配置的连接器创建一个名为`sales`的目录。

## 使用跨域动态过滤

启用跨域动态过滤，在执行跨openLooKeng查询时，在本端生成过滤器，并将过滤器发送到远端openLooKeng进行数据过滤，减少从远端openLooKeng拉取的数据量。需要先确保openLooKeng环境中有启用state-store（相关配置可参考state-store的配置文档）。启用跨域动态过滤有两种方式：

**方式一**： 可以在`etc/config.properties`中设置以下属性：

| 属性名称                                     | 说明                                                         | 默认值  |
| -------------------------------------------- | ------------------------------------------------------------ | ------- |
| `enable-dynamic-filtering`                   | 是否启用动态过滤特性                                         | `false` |
| `dynamic-filtering-max-per-driver-row-count` | 每个driver最大允许处理的行数，超过该行数，则该查询的动态过滤特性会自动取消 | `100`   |
| `dynamic-filtering-max-per-driver-size`      | 每个driver最大允许处理的数据量大小，超过该值，则该查询的动态过滤特性会自动取消 | `10KB`  |

**方式二**：通过设置session：

例1（通过CLI链接openLooKeng）：

   ` java -jar hetu-cli-*-execute.jar --server ip:port --session enable-dynamic-filter=ture --session dynamic-filtering-max-per-driver-row-count=10000 --session dynamic-filtering-max-per-driver-size=1MB`

例2（通过JDBC链接openLooKeng）:

```java
Properties properties = new Properties();
properites.setProperties("enable-dynamic-filter", "true");
properites.setProperties("dynamic-filtering-max-per-driver-row-count", "10000");
properites.setProperties("dynamic-filtering-max-per-driver-size", "1MB");

String url = "jdbc:lk://127.0.0.0:8090/hive/default";
Connection connection = DriverManager.getConnection(url, properties);

```


## 查询远程数据中心

数据中心连接器为远程数据中心中的每个*目录*提供一个以属性文件名为前缀的目录。将每个带前缀的远程目录视为本地集群中的独立目录。可以通过执行`SHOW CATALOGS`来查看可用远程目录：

    SHOW CATALOGS;

如果在远程数据中心中有一个名为`mysql`的目录，则可以通过执行`SHOW SCHEMAS`来查看远程目录中的模式：

    SHOW SCHEMAS FROM dc.mysql;

如果远程目录`mysql`中有一个名为`web`的模式，则可以通过执行`SHOW TABLES`来查看该目录中的表：

    SHOW TABLES FROM dc.mysql.web;

可以使用以下方法之一查看`web`模式中`clicks`表中的列的列表：

    DESCRIBE dc.mysql.web.clicks;
    SHOW COLUMNS FROM dc.mysql.web.clicks;

最后，可以访问`web`模式中的`clicks`表：

    SELECT * FROM dc.mysql.web.clicks;

如果对目录属性文件使用不同的名称，请使用该目录名称，而不要在上述示例中使用`dc`。


## 数据中心连接器限制

数据中心连接器是一个只读连接器。暂不支持以下SQL语句：

[ALTER SCHEMA](../sql/alter-schema.md)、[ALTER TABLE](../sql/alter-table.md)、[ANALYZE](../sql/analyze.md)、[CACHE TABLE](../sql/cache-table.md)、[COMMENT](../sql/comment.md)、[CREATE SCHEMA](../sql/create-schema.md)、[CREATE TABLE](../sql/create-table.md)、[CREATE TABLE AS](../sql/create-table-as.md)、[CREATE VIEW](../sql/create-view.md)、[DELETE](../sql/delete.md)、[DROP CACHE](../sql/drop-cache.md)、[DROP SCHEMA](../sql/drop-schema.md)、[DROP TABLE](../sql/drop-table.md)、[DROP VIEW](../sql/drop-view.md)、[GRANT](../sql/grant.md)、[INSERT](../sql/insert.md)、[INSERT OVERWRITE](../sql/insert-overwrite.md)、[REVOKE](../sql/revoke.md)、[SHOW CACHE](../sql/show-cache.md)、[SHOW CREATE VIEW](../sql/show-create-view.md)、[SHOW GRANTS](../sql/show-grants.md)、[SHOW ROLES](../sql/show-roles.md)、[SHOW ROLE GRANTS](../sql/show-role-grants.md)、[UPDATE](../sql/update.md)、[VACUUM](../sql/vacuum.md)