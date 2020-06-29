Data Center Connector
====================

The Data Center connector allows querying a remote openLooKeng data center.
This can be used to join data between different openLooKeng clusters from the  local openLooKeng environment.

Configuration
-------------

To configure the Data Center connector, create a catalog properties file in `etc/catalog` named, for example, `dc.properties`, to mount the Data Center connector as the `dc` catalog. Create the file with the following contents, replacing the connection properties as appropriate for your setup:  

``` properties
connector.name=dc
connection-url=http://example.net:8080
connection-user=root
connection-password=password
```

The following table lists all properties supported by the datacenter connector.

| Property Name                                   | Description                                                                                                                                                                                                                                                    | Default              |
|-------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------|
| `connection-url`                                | The connection URL                                                                                                                                                                                                                                             |                      |
| `connection-user`                               | Username                                                                                                                                                                                                                                                       |                      |
| `connection-password`                           | Password                                                                                                                                                                                                                                                       |                      |
| `dc.accesstoken`                                | Access token for token based authentication                                                                                                                                                                                                                    |                      |
| `dc.application.name.prefix`                    | Prefix to append to any specified ApplicationName client info property, which is used to Set source name for the openLooKeng query. If neither this property nor ApplicationName are set, the source for the query will be hetu-dc                                    | `hetu-dc`            |
| `dc.extra.credentials`                          | Extra credentials for connecting to external services. The extraCredentials is a list of key-value pairs. Example: foo:bar;abc:xyz will create credentials abc=xyz and foo=bar                                                                                 |                      |
| `dc.http-client-timeout`                        | Time until the client keeps retrying to fetch the data, default value is 10min                                                                                                                                                                                 | `10.00m`             |
| `dc.http-compression`                           | Whether use gzip compress response body, default value is true                                                                                                                                                                                                 | `false`              |
| `dc.http-request-connectTimeout`                | http request connect timeout, default value is 1min                                                                                                                                                                                                            | `30.00s`             |
| `dc.http-request-readTimeout`                   | http request read timeout, default value is 2min                                                                                                                                                                                                               | `30.00s`             |
| `dc.httpclient.maximum.idle.connections`        | Maximum idle connections to be kept open in the http client                                                                                                                                                                                                    | `20`                 |
| `dc.httpproxy`                                  | HTTP proxy host and port. Example: localhost:8888                                                                                                                                                                                                              |                      |
| `dc.kerberos.config.path`                       | Kerberos configuration file                                                                                                                                                                                                                                    |                      |
| `dc.kerberos.credential.cachepath`              | Kerberos credential cache                                                                                                                                                                                                                                      |                      |
| `dc.kerberos.keytab.path`                       | Kerberos keytab file                                                                                                                                                                                                                                           |                      |
| `dc.kerberos.principal`                         | The principal to use when authenticating to the openLooKeng coordinator                                                                                                                                                                                               |                      |
| `dc.kerberos.remote.service.name`               | openLooKeng coordinator Kerberos service name. This parameter is required for Kerberos authentication                                                                                                                                                                 |                      |
| `dc.kerberos.service.principal.pattern`         | openLooKeng coordinator Kerberos service principal pattern. The default is `${SERVICE}@${HOST}.${SERVICE}` is replaced with the value of KerberosRemoteServiceName and `${HOST}` is replaced with the hostname of the coordinator (after canonicalization if enabled) | `${SERVICE}@${HOST}` |
| `dc.kerberos.use.canonical.hostname`            | Use the canonical hostname of the openLooKeng coordinator for the Kerberos service principal by firstresolving the hostname to an IP address and then doing a reverse DNS lookup for that IP address.                                                                 | `false`              |
| `dc.max.anticipated.delay`                      | Maximum anticipated delay between two requests for a query in the cluster. If the remote dc did not receive a request for more than this delay, it may cancel the query.                                                                                       | `10.00m`             |
| `dc.metadata.cache.enabled`                     | Metadata Cache Enabled                                                                                                                                                                                                                                         | `true`               |
| `dc.metadata.cache.maximum.size`                | Metadata Cache Maximum Size                                                                                                                                                                                                                                    | `10000`              |
| `dc.metadata.cache.ttl`                         | Metadata Cache Ttl                                                                                                                                                                                                                                             | `1.00s`              |
| `dc.query.pushdown.enabled`                     | Enable sub-query push down to this data center. If this property is not set, by default sub-queries are pushed down                                                                                                                                            | `true`               |
| `dc.remote-http-server.max-request-header-size` | This property should be equivalent to the value of http-server.max-request-header-size in the remote server                                                                                                                                                    | `4kB`                |
| `dc.remote.cluster.id`                          | A unique id for the remote cluster                                                                                                                                                                                                                             |                      |
| `dc.socksproxy`                                 | SOCKS proxy host and port. Example: localhost:1080                                                                                                                                                                                                             |                      |
| `dc.ssl`                                        | Use HTTPS for connections                                                                                                                                                                                                                                      | `false`              |
| `dc.ssl.keystore.password`                      | The keystore password                                                                                                                                                                                                                                          |                      |
| `dc.ssl.keystore.path`                          | The location of the Java KeyStore file that contains the certificate and private key to use for authentication                                                                                                                                                 |                      |
| `dc.ssl.truststore.password`                    | The truststore password                                                                                                                                                                                                                                        |                      |
| `dc.ssl.truststore.path`                        | The location of the Java TrustStore file that will be used to validate HTTPS server certificates                                                                                                                                                               |                      |

### Multiple openLooKeng Clusters

You can have as many catalogs as you need, so if you have additional data centers, simply add another properties file to `etc/catalog` with a different name (making sure it ends in `.properties`). For example, if
you name the property file `sales.properties`, openLooKeng will create a catalog named `sales` using the configured connector.

Querying Remote Data Center
---------------------------

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

Data Center Connector Limitations
---------------------------------

Data Center connector is a read-only connector. The following SQL statements are not yet supported:

[alter-schema](../sql/alter-schema), [alter-table](../sql/alter-table), [analyze](../sql/analyze), [cache-table](../sql/cache-table), [comment](../sql/comment), [create-schema](../sql/create-schema), [create-table](../sql/create-table), [create-table-as](../sql/create-table-as), [create-view](../sql/create-view), [delete](../sql/delete), [drop-cache](../sql/drop-cache), [drop-schema](../sql/drop-schema), [drop-table](../sql/drop-table), [drop-view](../sql/drop-view), [grant](../sql/grant), [insert](../sql/insert), [insert-overwrite](../sql/insert-overwrite), [revoke](../sql/revoke), [revoke](../sql/revoke), [show-cache](../sql/show-cache), [show-create-view](../sql/show-create-view), [show-grants](../sql/show-grants), [show-roles](../sql/show-roles), [show-role-grants](../sql/show-role-grants), [update](../sql/update), [vacuum](../sql/vacuum)