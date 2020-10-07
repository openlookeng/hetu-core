
# JDBC Driver

openLooKeng can be accessed from Java using the JDBC driver. Add the jar to the classpath of your Java application.

Starting from version `1.0.1`, the driver is also available from Maven Central. Specify an appropriate version number:

```xml
<dependency>
    <groupId>io.hetu.core</groupId>
    <artifactId>hetu-jdbc</artifactId>
    <version>1.0.1</version>
</dependency>
```

## Driver Name

The driver class name is `io.hetu.core.jdbc.OpenLooKengDriver`. Most users will not need this information as drivers are loaded automatically.

## Connection

The following JDBC URL formats are supported:

```
jdbc:lk://host:port
jdbc:lk://host:port/catalog
jdbc:lk://host:port/catalog/schema
```

For example, use the following URL to connect to openLooKeng running on `example.net` port `8080` with the catalog `hive` and the schema `sales`:

```
jdbc:lk://example.net:8080/hive/sales
```

The above URL can be used as follows to create a connection:

``` java
String url = "jdbc:lk://example.net:8080/hive/sales";
Connection connection = DriverManager.getConnection(url, "test", null);
```

## Connection Parameters

The driver supports various parameters that may be set as URL parameters or as properties passed to `DriverManager`. Both of the following examples are equivalent:

``` java
// URL parameters
String url = "jdbc:lk://example.net:8080/hive/sales";
Properties properties = new Properties();
properties.setProperty("user", "test");
properties.setProperty("password", "secret");
properties.setProperty("SSL", "true");
Connection connection = DriverManager.getConnection(url, properties);

// properties
String url = "jdbc:lk://example.net:8080/hive/sales?user=test&password=secret&SSL=true";
Connection connection = DriverManager.getConnection(url);
```

These methods may be mixed; some parameters may be specified in the URL while others are specified using properties. However, the same parameter may not be specified using both methods.

## Parameter Reference

| Name                              | Description                                                  |
| :-------------------------------- | :----------------------------------------------------------- |
| `user`                            | Username to use for authentication and authorization.        |
| `password`                        | Password to use for LDAP authentication.                     |
| `socksProxy`                      | SOCKS proxy host and port. Example: `localhost:1080`         |
| `httpProxy`                       | HTTP proxy host and port. Example: `localhost:8888`          |
| `applicationNamePrefix`           | Prefix to append to any specified `ApplicationName` client info property, which is used to set the source name for the openLooKeng query. If neither this property nor `ApplicationName` are set, the source for the query will be `presto-jdbc`. |
| `accessToken`                     | Access token for token based authentication.                 |
| `SSL`                             | Use HTTPS for connections                                    |
| `SSLKeyStorePath`                 | The location of the Java KeyStore file that contains the certificate and private key to use for authentication. |
| `SSLKeyStorePassword`             | The password for the KeyStore.                               |
| `SSLTrustStorePath`               | The location of the Java TrustStore file that will be used to validate HTTPS server certificates. |
| `SSLTrustStorePassword`           | The password for the TrustStore.                             |
| `KerberosRemoteServiceName`       | openLooKeng coordinator Kerberos service name. This parameter is required for Kerberos authentication. |
| `KerberosPrincipal`               | The principal to use when authenticating to the openLooKeng coordinator. |
| `KerberosUseCanonicalHostname`    | Use the canonical hostname of the openLooKeng coordinator for the Kerberos service principal by first resolving the hostname to an IP address and then doing a reverse DNS lookup for that IP address. This is enabled by default. |
| `KerberosServicePrincipalPattern` | openLooKeng coordinator Kerberos service principal pattern. The default is `${SERVICE}@${HOST}`. `${SERVICE}` is replaced with the value of `KerberosRemoteServiceName` and `${HOST}` is replaced with the hostname of the coordinator (after canonicalization if enabled). |
| `KerberosConfigPath`              | Kerberos configuration file.                                 |
| `KerberosKeytabPath`              | Kerberos keytab file.                                        |
| `KerberosCredentialCachePath`     | Kerberos credential cache.                                   |
| `extraCredentials`                | Extra credentials for connecting to external services. The extraCredentials is a list of key-value pairs. Example: `foo:bar;abc:xyz` will create credentials `abc=xyz` and `foo=bar` |