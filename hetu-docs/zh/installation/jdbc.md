
# JDBC驱动

可以使用JDBC驱动从Java访问openLooKeng，并将其添加到Java应用的类路径中。

从版本`1.0.1`开始，该驱动也可从Maven Central获得。请指定合适的版本：

```xml
<dependency>
    <groupId>io.hetu.core</groupId>
    <artifactId>hetu-jdbc</artifactId>
    <version>1.0.1</version>
</dependency>
```

## 驱动名称

驱动类名为`io.hetu.core.jdbc.OpenLooKengDriver`。大多数用户不需要此信息，因为驱动是自动加载的。

## 连接

JDBC URL支持如下格式：

```
jdbc:lk://host:port
jdbc:lk://host:port/catalog
jdbc:lk://host:port/catalog/schema
```

例如，使用以下URL连接到运行于`example.net`端口`8080`的openLooKeng，其目录为`hive`，模式为`sales`：

```
jdbc:lk://example.net:8080/hive/sales
```

可以使用上面的URL创建连接，如下所示：

``` java
String url = "jdbc:lk://example.net:8080/hive/sales";
Connection connection = DriverManager.getConnection(url, "test", null);
```

## 连接参数

该驱动支持各种参数，这些参数可以设置为URL参数或作为传递给`DriverManager`的属性。以下两个示例是等效的：

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

这些方法可以混合使用；一些参数可以在URL中指定，而另一些参数则使用属性指定。但是相同的参数不可同时用两种方法指定。

## 参数参考

| 项目| 描述|
|:----------|:----------|
| `user`| 用于身份验证和授权的用户名。|
| `password`| 用于LDAP身份验证的密码。|
| `socksProxy`| SOCKS代理主机和端口。示例：`localhost:1080`|
| `httpProxy`| HTTP代理主机和端口。示例：`localhost:8888`|
| `applicationNamePrefix`| 添加到任何指定的`ApplicationName`客户端信息属性的前缀，该前缀用于设置openLooKeng查询的源名称。如果既没有设置该属性也没有设置`ApplicationName`，则查询的源将是`presto-jdbc`。|
| `accessToken`| 基于令牌身份验证的访问令牌。|
| `SSL`| 使用HTTPS连接。|
| `SSLKeyStorePath`| 包含用于身份验证的证书和私钥的JavaKeyStore文件的位置。|
| `SSLKeyStorePassword`| KeyStore密码。|
| `SSLTrustStorePath`| 用于验证HTTPS服务器证书的Java TrustStore文件的位置。|
| `SSLTrustStorePassword`| TrustStore密码。|
| `KerberosRemoteServiceName`| openLooKeng协调节点Kerberos服务的名称。Kerberos认证时，需要配置该参数。|
| `KerberosPrincipal`| 向openLooKeng协调节点进行身份验证时使用的主体。|
| `KerberosUseCanonicalHostname`| 通过首先将主机名解析为IP地址，然后对该IP地址执行反向DNS查找，从而使用Kerberos服务主体的openLooKeng协调节点的规范主机名。此参数默认启用。|
| `KerberosServicePrincipalPattern`| openLooKeng协调节点Kerberos服务主体模式。默认值为`${SERVICE}@${HOST}`。`${SERVICE}`替换为`KerberosRemoteServiceName`的值，`${HOST}`替换为协调节点的主机名（如果启用了规范化，则为规范化后的主机名）。|
| `KerberosConfigPath`| Kerberos配置文件。|
| `KerberosKeytabPath`| Kerberos keytab文件。|
| `KerberosCredentialCachePath`| Kerberos凭证缓存。|
| `extraCredentials`| 连接外部服务的额外凭证。ExtraCredentials是一个键值对列表。示例：`foo:bar;abc:xyz`将创建凭据`abc=xyz`和`foo=bar`。|

