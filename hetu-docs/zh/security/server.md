
协调节点Kerberos认证
===================================

可以配置openLooKeng协调节点，为客户端（例如openLooKeng命令行或JDBC和ODBC驱动程序）启用基于HTTPS的Kerberos身份验证。

 

为了启用Kerberos身份验证，需要修改在openLooKeng协调节点上的配置。不需要修改在工作节点上的配置。工作节点将继续通过未经身份验证的HTTP连接到协调节点。但是，如果需要确保openLooKeng节点之间使用SSL/TLS通信，则需要配置“内部通信安全”。


环境配置
-------------------------

### Kerberos服务

openLooKeng协调节点通过网络访问的节点上需要运行Kerberos KDC（密钥分发中心）。KDC负责对主体进行身份验证，并为启用Kerberos的服务发布会话密钥。KDC通常在端口88上运行，该端口是IANA为Kerberos分配的端口。

 MIT Kerberos配置介绍

Kerberos需要在openLooKeng协调节点上配置。至少需要在`/etc/krb5.conf`文件中的`[realms]`小节中有一个`kdc`条目。还可以增加一个`admin_server`条目，确保openLooKeng协调节点能够访问端口749上的Kerberos管理服务器。

```
[realms]
  OPENLOOKENG.EXAMPLE.COM = {
    kdc = kdc.example.com
    admin_server = kdc.example.com
  }

[domain_realm]
  .openlookeng.example.com = OPENLOOKENG.EXAMPLE.COM
  openlookeng.example.com = OPENLOOKENG.EXAMPLE.COM
```

Krb5.conf的完整[文档](http://web.mit.edu/kerberos/krb5-latest/doc/admin/conf_files/kdc_conf.html )由麻省理工学院Kerberos项目托管。如果您使用Kerberos协议的不同实现方式，则需要根据您的环境修改配置。

### Kerberos主体和Keytab文件

openLooKeng协调节点需要一个Kerberos主体，需要连接到openLooKeng协调节点的用户也需要一个Kerberos主体。可以使用[kadmin](http://web.mit.edu/kerberos/krb5-latest/doc/admin/admin_commands/kadmin_local.html )在Kerberos中创建这些用户。

另外，openLooKeng协调节点需要一个 [keytab文件](http://web.mit.edu/kerberos/krb5-devel/doc/basic/keytab_def.html )。创建主体后，可以使用**kadmin**创建keytab文件。

```
kadmin
> addprinc -randkey openlookeng@EXAMPLE.COM
> addprinc -randkey openlookeng/openlookeng-coordinator.example.com@EXAMPLE.COM
> ktadd -k /etc/openlookeng/openlookeng.keytab openlookeng@EXAMPLE.COM
> ktadd -k /etc/openlookeng/openlookeng.keytab openlookeng/openlookeng-coordinator.example.com@EXAMPLE.COM
```

**注意**

*运行**ktadd**会使主体的密钥随机化。如果您刚刚创建了主体，这无关紧要。如果主体已经存在，并且现有用户或服务依赖于能够使用密码或keytab进行验证，则运行**ktadd**时增加`-norandkey`选项。*

### Java加密扩展策略文件

JRE自带策略文件以限制可使用的加密密钥的强度。但默认情况下，Kerberos使用的密钥比策略文件支持的密钥要大。有两种可能的解决方案：

> - 更新JCE策略文件。
> - 配置Kerberos，使用强度降低的密钥。

推荐采用更新JCE策略文件的方案。JCE策略文件可以从Oracle下载。请注意，JCE策略文件因Java主版本而异。例如，Java6的策略文件不能在Java 8中使用。

Java 8策略文件可以从[这里](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html  )获取。ZIP归档文件的`README`文件中有安装策略文件的说明。如果要在系统JRE中安装策略文件，则需要管理级别的访问权限。

### TLS使用的Java 密钥库文件

使用Kerberos身份验证时，应该通过HTTPS访问openLooKeng协调节点。您可以通过在协调节点上创建用于TLS连接的Java密钥库文件来实现。

 

## 系统访问控制插件

启用Kerberos的openLooKeng协调节点可能需要一个系统访问控制插件来达到所需的安全级别。

 

## openLooKeng协调节点配置

在配置openLooKeng协调节点使用Kerberos身份验证和HTTPS之前，必须对环境进行上述修改。在完成以下环境修改后，您可以修改openLooKeng配置文件。

- Kerberos服务
- MIT Kerberos配置
- Kerberos主体和Keytab文件
- 用于TLS的Java密钥库文件
- 系统访问控制插件

### config.properties

在协调节点的`config.properties`文件中对Kerberos认证进行配置。需要添加的表项如下：

```properties
http-server.authentication.type=KERBEROS

http.server.authentication.krb5.service-name=openlookeng
http.server.authentication.krb5.principal-hostname=openlookeng.example.com
http.server.authentication.krb5.keytab=/etc/openlookeng/openlookeng.keytab
http.authentication.krb5.config=/etc/krb5.conf

http-server.https.enabled=true
http-server.https.port=7778

http-server.https.keystore.path=/etc/openlookeng_keystore.jks
http-server.https.keystore.key=keystore_password
```

| 属性                                             | 描述                                                  |
| :--------------------------------------------------- | :----------------------------------------------------------- |
| `http-server.authentication.type` | openLooKeng协调节点的认证类型。必须设置为`KERBEROS`。|
| `http.server.authentication.krb5.service-name` | openLooKeng协调节点的Kerberos服务名。服务名必须与Kerberos主体匹配。|
| `http.server.authentication.krb5.principal-hostname` |openLooKeng协调节点的的Kerberos主机名。主机名必须与Kerberos主体匹配。该参数为可选参数。如果设置，openLooKeng将在Kerberos主体的主机部分使用这个值，而不是使用机器的主机名。|
| `http.server.authentication.krb5.keytab` | 用来对Kerberos主体进行身份验证的keytab文件的位置。|
| `http.authentication.krb5.config` | kerberos配置文件所在的位置。             |
| `http-server.https.enabled` | 为openLooKeng协调节点开启HTTPS访问功能。取值设置为`true`。|
| `http-server.https.port` | HTTPS服务器的端口号。                                           |
| `http-server.https.keystore.path` | 用于TLS安全连接的Java密钥库文件的位置。|
| `http-server.https.keystore.key` |密钥库的密码。必须与创建密钥库时设置的密码一致。|
| `http-server.authentication.krb5.user-mapping.pattern` | 用于认证用户匹配的正则表达式。如果匹配，认证用户映射到正则表达式中的第一个匹配组；如果不匹配，则拒绝认证。默认值是`(.*)`。 |
| `http-server.authentication.krb5.user-mapping.file`    | 包含用户映射规则的JSON文件。详见 [认证用户映射](./user-mapping.md)。 |

注意

`http-server.authentication.krb5.user-mapping.pattern`和`http-server.authentication.krb5.user-mapping.file`属性不能同时设置。

开启HTTPS后，监控openLooKeng协调节点的CPU使用率。如果您允许Java从大的列表中选择，那么它更喜欢CPU密集型的加密套件。启用HTTPS后，如果CPU占用率过高，可以通过设置`http-server.https.included-cipher`属性只允许廉价的密码，使Java使用指定的加密套件。非前向保密密码默认关闭。因此，如果您想选择非前向保密密码，您需要将`http-server.https.excluded-cipher`属性设置为空列表，以覆盖默认的排除。

```
http-server.https.included-cipher=TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA256
http-server.https.excluded-cipher=
```

Java资料中列出了[支持的加密套件](http://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html#SupportedCipherSuites )。

### access-controls.properties

`access-control.properties`文件必须至少包含`access-control.name`属性。  其他配置则因配置的实现而异。有关详细信息，请参阅系统访问控制。

 



## 异常处理

实现Kerberos身份验证具有挑战性的。您可以独立地验证openLooKeng之外的一些配置，以便在尝试解决问题时缩小您的关注范围。

### Kerberos验证

请确保openLooKeng协调节点能够通过**telnet**连接到KDC。

```
$ telnet kdc.example.com 88
```

验证使用keytab文件通过[kinit](http://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html )和[klist](http://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/klist.html )成功获取票证功能。

```
$ kinit -kt /etc/openlookeng/openlookeng.keytab openlookeng@EXAMPLE.COM
$ klist
```

### Java密钥库文件验证

使用[Java密钥库文件验证](tls.md#java密钥库文件验证)中的方法验证密钥库文件的密码并查看其内容。

### Kerberos附加调试信息

通过在openLooKeng `jvm.config`文件中添加以下行，可以为openLooKeng协调节点进程启用额外的Kerberos调试信息：

```
-Dsun.security.krb5.debug=true
-Dlog.enable-console=true
```

`-Dsun.security.krb5.debug=true`启用来自JRE Kerberos库的Kerberos调试输出。调试输出进入`stdout`，再被openLooKeng重定向到日志记录系统。`-Dlog.enable-console=true`使输出到`stdout`的输出呈现在日志中。

Kerberos调试输出发送到日志的信息量和有用性因身份验证失败的位置而异。异常消息和堆栈跟踪也可以提供有关问题本质的有用线索。



### 更多资源

[常见的Kerberos错误信息(A-M)](http://docs.oracle.com/cd/E19253-01/816-4557/trouble-6/index.html)

[常见的Kerberos错误信息(N-Z)](http://docs.oracle.com/cd/E19253-01/816-4557/trouble-27/index.html)

[MIT Kerberos文档：故障处理](http://web.mit.edu/kerberos/krb5-latest/doc/admin/troubleshoot.html)
