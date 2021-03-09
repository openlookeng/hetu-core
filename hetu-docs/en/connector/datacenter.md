# Data Center Connector

The Data Center connector allows querying a remote openLooKeng data center. This can be used to join data between different openLooKeng clusters from the local openLooKeng environment.

## Local DC Connector Configuration

To configure the Data Center connector, create a catalog properties file in `etc/catalog` named, for example, `<dc-name>.properties`, to mount the Data Center connector as the `<dc-name>` catalog. Create the file with the following contents, replacing the `connection` properties as appropriate for your setup:

### Basic Configuration

``` properties
connector.name=dc
connection-url=http://example.net:8080
connection-user=<The User Name of Remote openLooKeng>
connection-password=<The Password of remote openLooKeng>
```

| Property Name         | Description                              | Required | Default Value |
| --------------------- | ---------------------------------------- | :------- | ------------- |
| `connection-url`      | URL of openLooKeng cluster to connect       | Yes      |               |
| `connection-user`     | User Name to use when connecting to openLooKeng cluster | No      |               |
| `connection-password` | Password to use when connecting to openLooKeng cluster  | No       |               |

### Security Configuration

When the remote openLooKeng has enabled security authentication or TLS / SSL, the corresponding security configuration should be carried out on the `<dc-name>.properties`.

#### Kerberos Authentication Mode

| Property Name                           | Description                                                  | Default Value        |
| --------------------------------------- | ------------------------------------------------------------ | -------------------- |
| `dc.kerberos.config.path`               | Kerberos configuration file                                  |                      |
| `dc.kerberos.credential.cachepath`      | Kerberos credential cache                                    |                      |
| `dc.kerberos.keytab.path`               | Kerberos keytab file                                         |                      |
| `dc.kerberos.principal`                 | The principal to use when authenticating to the openLooKeng coordinator |                      |
| `dc.kerberos.remote.service.name`       | openLooKeng coordinator Kerberos service name. This parameter is required for Kerberos authentication |                      |
| `dc.kerberos.service.principal.pattern` | openLooKeng coordinator Kerberos service principal pattern. The default is `${SERVICE}@${HOST}.${SERVICE}` is replaced with the value of `dc.kerberos.remote.service.name` and `${HOST}` is replaced with the host name of the coordinator (after canonicalization if enabled) | `${SERVICE}@${HOST}` |
| `dc.kerberos.use.canonical.hostname`    | Use the canonical host name of the openLooKeng coordinator for the Kerberos service principal by first resolving the host name to an IP address and then doing a reverse DNS lookup for that IP address. | `false`              |

#### Token Authentication Mode

| Property Name    | Description                                 | Default Value |
| ---------------- | ------------------------------------------- | ------------- |
| `dc.accesstoken` | Access token for token based authentication |               |

#### External Certificate Authentication Mode

| Property Name          | Description                                                  | Default Value |
| ---------------------- | ------------------------------------------------------------ | ------------- |
| `dc.extra.credentials` | Extra credentials for connecting to external services. The `extra.credentials` is a list of key-value pairs. Example: foo:bar;abc:xyz will create credentials abc=xyz and foo=bar |               |

#### SSL/TLS Configuration

| Property Name                | Description                                                  | Default Value |
| ---------------------------- | ------------------------------------------------------------ | ------------- |
| `dc.ssl`                     | Use HTTPS for connections                                    | `false`       |
| `dc.ssl.keystore.password`   | The keystore password                                        |               |
| `dc.ssl.keystore.path`       | The location of the Java keystore file that contains the certificate and private key to use for authentication |               |
| `dc.ssl.truststore.password` | The truststore password                                      |               |
| `dc.ssl.truststore.path`     | The location of the Java truststore file that will be used to validate HTTPS server certificates |               |

### Proxy Configuration

| Property Name   | Description                                        | Default Value |
| --------------- | -------------------------------------------------- | ------------- |
| `dc.socksproxy` | SOCKS proxy host and port. Example: localhost:1080 |               |
| `dc.httpproxy`  | HTTP proxy host and port. Example: localhost:8888  |               |

### Performance Optimization Configuration

| Property Name                    | Description                                                  | Default Value |
| -------------------------------- | ------------------------------------------------------------ | ------------- |
| `dc.metadata.cache.enabled`      | Metadata Cache Enabled                                       | `true`        |
| `dc.metadata.cache.maximum.size` | Metadata Cache Maximum Size                                  | `10000`       |
| `dc.metadata.cache.ttl`          | Metadata Cache TTL                                           | `1.00s`       |
| `dc.query.pushdown.enabled`      | Enable sub-query push down to this data center. If this property is not set, by default sub-queries are pushed down | `true`        |
| `dc.query.pushdown.module`      | FULL_PUSHDOWN: All push down. BASE_PUSHDOWN: Partial push down, which indicates that filter, aggregation, limit, topN and project can be pushed down. | `FULL_PUSHDOWN`        |
| `dc.http-compression`            | Whether use zstd compress response body, default value is false | `false`       |

### Other Properties

| Property Name                                   | Description                                                  | Default Value |
| ----------------------------------------------- | ------------------------------------------------------------ | ------------- |
| `dc.http-request-connectTimeout`                | HTTP request connect timeout, default value is 30s           | `30.00s`      |
| `dc.http-request-readTimeout`                   | HTTP request read timeout, default value is 30s              | `30.00s`      |
| `dc.httpclient.maximum.idle.connections`        | Maximum idle connections to be kept open in the HTTP client  | `20`          |
| `dc.http-client-timeout`                        | Time until the client keeps retrying to fetch the data, default value is 10 min | `10.00m`      |
| `dc.max.anticipated.delay`                      | Maximum anticipated delay between two requests for a query in the cluster. If the remote openLooKeng did not receive a request for more than this delay, it may cancel the query | `10.00m`      |
| `dc.application.name.prefix`                    | Prefix to append to any specified ApplicationName client info property, which is used to Set source name for the openLooKeng query. If neither this property nor ApplicationName are set, the source for the query will be hetu-dc | `hetu-dc`     |
| `dc.remote-http-server.max-request-header-size` | This property should be equivalent to the value of `http-server.max-request-header-size` in the remote server |               |
| `dc.remote.cluster.id`                          | A unique id for the remote cluster                           |               |

## Remote openLooKeng Configuration

### openLooKeng Configuration

You can set following properties in the `etc/config.properties`:

| Property Name                       | Description                                                  | Default Value |
| ----------------------------------- | ------------------------------------------------------------ | ------------- |
| `hetu.data.center.split.count`      | Maximum number of splits allowed per query                   | `5`           |
| `hetu.data.center.consumer.timeout` | The maximum delay of waiting to be taken after the data is obtained by executing the query | `10min`       |

### Nginx Configuration

When HA is enabled at the remote end and Nginx is used as the proxy, the configuration of Nginx needs to be modified: 

```nginx
http {
    upstream for_aa {
        ip_hash;
        server 192.168.0.101:8090;   #coordinator-1;
        server 192.168.0.102:8090;   #coordinator-2;
        check interval=3000 rise=2 fall=5 timeout=1000 type=http;
    }

    upstream for_cross_region {
        hash $hashKey consistent;
        server 192.168.0.101:8090;   #coordinator-1;
        server 192.168.0.102:8090;   #coordinator-2;
        check interval=3000 rise=2 fall=5 timeout=1000 type=http;
    }
    
    server {
        listen nginx_ip:8888; # nginx port
        
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

## Multiple openLooKeng Clusters

You can have as many catalogs as you need, so if you have additional data centers, simply add another properties file to `etc/catalog` with a different name (making sure it ends in `.properties`). For example, if you name the property file `sales.properties`, openLooKeng will create a catalog named `sales` using the configured connector.

## Global Dynamic Filter

The global dynamic filtering is enabled, when the cross openLooKeng query is executed, the filter is generated locally and sent to the remote openLooKeng for data filtering to reduce the amount of data pulled from the remote openLooKeng. It is necessary to ensure that `state store` is enabled in openLooKeng environment (please refer to the configuration document of state store for relevant configuration). There are two ways to enable global dynamic filtering:

**Method 1**: You can set following properties in the `etc/config.properties`:

| Property Name                                | Description                                                  | Default Value |
| -------------------------------------------- | ------------------------------------------------------------ | ------------- |
| `enable-dynamic-filtering`                   | Whether the dynamic filtering feature is enabled             | `false`       |
| `dynamic-filtering-max-per-driver-row-count` | If the maximum number of rows per driver is exceeded, the dynamic filtering feature of the query will be automatically cancelled | `100`         |
| `dynamic-filtering-max-per-driver-size`      | If the maximum amount of data allowed to be processed by each driver exceeds this value, the dynamic filtering feature of the query will be automatically cancelled | `10KB`        |

**Method 2**: Set properties in session

* By openLooKeng CLI

  ```shell
  java -jar hetu-cli-*-execute.jar --server ip:port --session enable-dynamic-filter=ture --session dynamic-filtering-max-per-driver-row-count=10000 --session dynamic-filtering-max-per-driver-size=1MB
  ```

* By openLooKeng JDBC:

  ```java
  Properties properties = new Properties();
  properites.setProperties("enable-dynamic-filter", "true");
  properites.setProperties("dynamic-filtering-max-per-driver-row-count", "10000");
  properites.setProperties("dynamic-filtering-max-per-driver-size", "1MB");
  
  String url = "jdbc:lk://127.0.0.0:8090/hive/default";
  Connection connection = DriverManager.getConnection(url, properties);
  ```

## Querying Remote Data Center

The Data Center connector provides a catalog prefixed with the property file name for every *catalog* in the remote data center. Treat each prefixed remote catalogs as a separate catalog in the local cluster. You can see the available remote catalogs by running `SHOW CATALOGS`:

    SHOW CATALOGS;

If you have catalog named `mysql` in the remote data center, you can view the schemas in this remote catalog by running `SHOW SCHEMAS`:

    SHOW SCHEMAS FROM dc.mysql;

If you have a schema named `web` in the remote catalog `mysql`, you can view the tables in that catalog by running ``SHOW TABLES``:

    SHOW TABLES FROM dc.mysql.web;

You can see a list of the columns in the `clicks` table in the `web` schema using either of the following:

    DESCRIBE dc.mysql.web.clicks;
    SHOW COLUMNS FROM dc.mysql.web.clicks;

Finally, you can access the `clicks` table in the `web` schema:

    SELECT * FROM dc.mysql.web.clicks;

If you used a different name for your catalog properties file, use that catalog name instead of `dc` in the above examples.

## Data Center Connector Limitations

Data Center connector is a read-only connector. The following SQL statements are not yet supported:

[ALTER SCHEMA](../sql/alter-schema.md), [ALTER TABLE](../sql/alter-table.md), [ANALYZE](../sql/analyze.md), [CACHE TABLE](../sql/cache-table.md), [COMMENT](../sql/comment.md), [CREATE SCHEMA](../sql/create-schema.md), [CREATE TABLE](../sql/create-table.md), [CREATE TABLE AS](../sql/create-table-as.md), [CREATE VIEW](../sql/create-view.md), [DELETE](../sql/delete.md), [DROP CACHE](../sql/drop-cache.md), [DROP SCHEMA](../sql/drop-schema.md), [DROP TABLE](../sql/drop-table.md), [DROP VIEW](../sql/drop-view.md), [GRANT](../sql/grant.md), [INSERT](../sql/insert.md), [INSERT OVERWRITE](../sql/insert-overwrite.md), [REVOKE](../sql/revoke.md), [SHOW CACHE](../sql/show-cache.md), [SHOW CREATE VIEW](../sql/show-create-view.md), [SHOW GRANTS](../sql/show-grants.md), [SHOW ROLES](../sql/show-roles.md), [SHOW ROLE GRANTS](../sql/show-role-grants.md), [UPDATE](../sql/update.md), [VACUUM](../sql/vacuum.md)

