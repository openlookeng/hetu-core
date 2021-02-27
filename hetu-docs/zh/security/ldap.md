
LDAP认证
===================

通过配置openLooKeng，可以为客户端（如`cli_ldap`）或JDBC、ODBC驱动启用HTTPS访问的前端LDAP认证。目前只支持涉及用户名和密码的简单的LDAP认证机制。openLooKeng客户端将用户名和密码发送给协调节点，协调节点使用外部LDAP服务验证这些凭据。

为了给openLooKeng启用LDAP身份验证，需要修改openLooKeng协调节点上的配置。Worker节点上的配置不需要更改，只为仅验证从客户端到协调节点的通信。但是，如果您希望使用SSL/TLS来确保openLooKeng节点之间的通信安全，需要配置`/security/internal- communication`。

openLooKeng服务器配置
-------------------------

### 环境配置

#### 安全LDAP协议

openLooKeng需要安全的LDAP (LDAPS)协议，所以请确保在LDAP服务器上启用了TLS。

#### openLooKeng协调节点的TLS配置

您需要将LDAP服务器的TLS证书导入到openLooKeng协调节点默认的Java信任库中，以保证TLS连接的安全。可以使用下面的示例中的`keytool`命令将证书`ldap_server.crt`导入到协调节点上的信任库中。

``` shell
$ keytool -import -keystore <JAVA_HOME>/jre/lib/security/cacerts -trustcacerts -alias ldap_server -file ldap_server.crt
```

除此之外，使用HTTPS访问openLooKeng协调节点。您可以通过在协调节点上创建`server_java_keystore`来实现。


### openLooKeng协调节点配置

在配置openLooKeng协调节点使用LDAP身份验证和HTTPS之前，必须对环境进行下列更改:

> -   `ldap_server`
> -   `server_java_keystore`

您还需要对openLooKeng配置文件进行修改。LDAP认证在协调节点上分为两部分进行配置。第一部分是在协调节点的`config.properties`文件中启用HTTPS和密码认证。
第二部分是将LDAP配置为密码验证器插件。

#### 服务器配置属性

以下示例列出了需要在协调节点的`config.properties`文件中添加的属性：

``` properties
http-server.authentication.type=PASSWORD

http-server.https.enabled=true
http-server.https.port=8443

http-server.https.keystore.path=/etc/openLooKeng.jks
http-server.https.keystore.key=keystore_password
```

 

| 属性                                                       | 描述                                                  |
| :--------------------------------------------------------- | :----------------------------------------------------------- |
| `http-server.authentication.type`                          | 对openLooKeng协调节点开启密码认证功能。必须设置为`PASSWORD`。|
| `http-server.https.enabled`                                | 对openLooKeng协调节点开启HTTPS访问功能。取值设置为`true`。默认值为`false`。|
| `http-server.https.port`                                   | HTTPS服务器的端口号。                                           |
| `http-server.https.keystore.path`                          | 用于保证TLS安全连接的Java密钥库文件的位置。|
| `http-server.https.keystore.key`                           | 密钥库的密码。必须与创建密钥库时设置的密码一致。|
| `http-server.authentication.password.user-mapping.pattern` | 用于认证用户匹配的正则表达式。如果匹配，认证用户映射到正则表达式中的第一个匹配组；如果不匹配，则拒绝认证。默认值是`(.*)`。 |
| `http-server.authentication.password.user-mapping.file`    | 包含用户映射规则的JSON文件。详见 [认证用户映射](./user-mapping.md)。 |

注意

`http-server.authentication.password.user-mapping.pattern`和`http-server.authentication.password.user-mapping.file`不能同时设置。

#### 密码验证器配置

配置密码认证使用LDAP协议。在协调节点上创建`etc/password-authenticator.properties`文件。
示例：

``` properties
password-authenticator.name=ldap
ldap.url=ldaps://ldap-server:636
ldap.user-bind-pattern=<Refer below for usage>
```

|属性                 |描述                                                  |
| :----------------------- | :----------------------------------------------------------- |
| `ldap.url` |指向LDAP服务器的URL。由于openLooKeng只允许使用安全的LDAP，所以url模式必须是`ldaps://`。|
| `ldap.user-bind-pattern` | 该属性可用于为密码验证指定LDAP用户绑定字符串。该属性必须包含`${USER}`，在密码验证期间，该字段将被实际的用户名替换。例如: `${USER}@corp.example.com`.|

根据LDAP服务器的实现类型，可以使用属性`ldap.user-bind-pattern`，如下所示。

##### 活动目录

``` properties
ldap.user-bind-pattern=${USER}@<domain_name_of_the_server>
```

示例：

``` properties
ldap.user-bind-pattern=${USER}@corp.example.com
```

##### OpenLDAP协议

``` properties
ldap.user-bind-pattern=uid=${USER},<distinguished_name_of_the_user>
```

示例：

``` properties
ldap.user-bind-pattern=uid=${USER},OU=America,DC=corp,DC=example,DC=com
```

#### 基于LDAP群组组成员的授权

除了基本的LDAP身份验证属性之外，还可以通过设置可选的`ldap.group-auth-pattern`和`ldap.user-base-dn`属性，进一步根据组成员身份限制允许连接到openLooKeng协调节点的用户集。

|属性                  | 描述                                                  |
| :------------------------ | :----------------------------------------------------------- |
| `ldap.user-base-dn` |尝试连接到服务器的用户的LDAP基本标识名。例如: `OU=America,DC=corp,DC=example,DC=com` |
| `ldap.group-auth-pattern` | 该属性用于指定LDAP群组成员授权的LDAP查询。该查询将针对LDAP服务器执行，如果成功，用户将获得授权。此属性必须包含一个`${USER}`，它将在群组授权搜索查询中被替换为实际的用户名。见以下示例。|

 根据LDAP服务器的实现类型，可以使用属性`ldap.group-auth-pattern`，如下所示。

##### 活动目录

```
ldap.group-auth-pattern=(&(objectClass=<objectclass_of_user>)(sAMAccountName=${USER})(memberof=<dn_of_the_authorized_group>))
```

示例：

```
ldap.group-auth-pattern=(&(objectClass=person)(sAMAccountName=${USER})(memberof=CN=AuthorizedGroup,OU=Asia,DC=corp,DC=example,DC=com))
```

##### OpenLDAP协议

```
ldap.group-auth-pattern=(&(objectClass=<objectclass_of_user>)(uid=${USER})(memberof=<dn_of_the_authorized_group>))
```

示例：

```
ldap.group-auth-pattern=(&(objectClass=inetOrgPerson)(uid=${USER})(memberof=CN=AuthorizedGroup,OU=Asia,DC=corp,DC=example,DC=com))
```

对于OpenLDAP，要使这个查询起作用，请确保启用`memberOf` [overlay](http://www.openldap.org/doc/admin24/overlays.html)。

也可以使用此属性对用户进行基于复杂的群组授权搜索查询的授权。例如，如果要对属于多个组（在OpenLDAP中）中的任何一个组的用户进行授权，则此属性可以设置为：

```
ldap.group-auth-pattern=(&(|(memberOf=CN=normal_group,DC=corp,DC=com)(memberOf=CN=another_group,DC=com))(objectClass=inetOrgPerson)(uid=${USER}))
```

openLooKeng命令行接口
--------

### 环境配置

#### TLS配置

使用LDAP身份验证时，应该使用HTTPS访问openLooKeng协调节点。openLooKeng命令行接口可以使用`Java Keystore <server_java_keystore>`文件或`Java Truststore <cli_java_truststore>`文件进行TLS配置。

如果您使用keystore文件，可以将它复制到客户端，并用于它进行TLS配置。如果您正在使用truststore文件，则可以使用默认的Java truststore，也可以在命令行界面创建自定义的truststore。不建议在生产中使用自签名证书。

### openLooKeng命令行执行

如果命令行接口开启了LDAP支持，要调用此接口，除了要配置连接不需要LDAP身份验证的openLooKeng协调节点所需的选项之外，还需要配置其它一些命令行选项。您可以使用`--keystore-*`属性或`--truststore-*`属性来确保安全的TLS连接。调用CLI的最简单方法是使用包装脚本。

``` shell
#!/bin/bash

./openlk-cli \
--server https://openlookeng-coordinator.example.com:8443 \
--keystore-path /tmp/openLooKeng.jks \
--keystore-password password \
--truststore-path /tmp/openLooKeng_truststore.jks \
--truststore-password password \
--catalog <catalog> \
--schema <schema> \
--user <LDAP user> \
--password
```

| 选项                  | 说明                                                  |
| :---------------------- | :----------------------------------------------------------- |
| `--server` | openLooKeng协调节点的地址和端口。  端口必须设置为openLooKeng协调节点侦听HTTPS连接的端口。使用LDAP认证时，openLooKeng命令行接口不支持使用`http`模式的URL。|
| `--keystore-path` |用于确保安全TLS连接的Java 密钥库（Keystore）文件的位置。|
| `--keystore-password` | 密钥库的密码。必须与创建密钥库时设置的密码一致。|
| `--truststore-path` |用来确保安全TLS连接的Java 信任库（Truststore）文件的位置。|
| `--truststore-password` | 信任库的密码。必须与创建信任库时设置的密码一致。|
| `--user` | LDAP用户名。对于活动目录，它应该是您的`sAMAccountName`；对于OpenLDAP，它应该是用户的`uid`。它是替换`config.properties`文件中配置的属性中的`${USER}`的用户名。|
| `--password` | 提示用户输入`user`的密码。                       |

异常处理
---------------

### Java密钥库文件验证

使用[Java密钥库文件验证](tls.md#java密钥库文件验证)中的方法验证密钥库文件的密码并查看其内容。

### openLooKeng命令行的SSL调试

如果运行openLooKeng CLI时遇到SSL相关的错误，可以使用`-Djavax.net.debug=ssl`参数运行CLI进行调试。 

使用openLooKeng 命令行接口可执行jar来启用它。

示例：

``` shell
java -Djavax.net.debug=ssl \
-jar \
hetu-cli-<version>-executable.jar \
--server https://coordinator:8443 \
<other_cli_arguments>
```

#### 常见SSL错误

##### java.security.cert.CertificateException: No subject alternative names present

当openLooKeng协调节点的证书无效，且在命令行接口`--server`参数中没有您指定的的IP地址时，就会出现此错误。您必须重新生成协调节点的SSL证书，并添加适当的`SAN（使用者可选名称）`。

如果`https://`使用URL中的IP地址而不是协调节点证书中包含的域，且证书中不包含`SAN (使用者可选名称)`参数，其对应的IP地址作为备选属性，则需要将SAN添加到此证书中。

#### JDK升级相关的认证或SSL错误

为了提高LDAPS（LDAP over TLS）连接的健壮性， 从JDK 8u181版本开始，默认启用端点识别算法。请参见随版本发布的版本说明书[这里](https://www.oracle.com/technetwork/java/javase/8u181-relnotes-4479407.html#JDK-8200666.)。

openLooKeng协调节点（运行JDK版本\>= 8u181）上的同一个LDAP服务器证书，以前能够成功连接到LDAPS服务器，现在可能失败，并出现以下错误：

```
javax.naming.CommunicationException: simple bind failed: ldapserver:636
[Root exception is javax.net.ssl.SSLHandshakeException: java.security.cert.CertificateException: No subject alternative DNS name matching ldapserver found.]
```

如果需要暂时关闭端点识别功能，可以在openLooKeng的`jvm.config`文件中增加`-Dcom.sun.jndi.ldap.object.disableEndpointIdentification=true`属性。 

但是，在生产环境中，我们建议通过重新生成LDAP服务器证书来修复该问题，使证书`SAN (Subject Alternative Name)` 或证书使用者名称与LDAP服务器匹配。
