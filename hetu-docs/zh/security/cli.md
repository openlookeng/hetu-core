
CLI接口的Kerberos认证
===========================

openLooKeng命令行接口可以连接到启用了Kerberos认证的openLooKeng协调节点。

 

## 环境配置

### Kerberos服务

客户端通过网络访问的节点上需要运行Kerberos KDC（密钥分发中心）。KDC负责对主体进行身份验证，并为启用Kerberos的服务发布会话密钥。KDC通常在端口88上运行，该端口是IANA为Kerberos分配的端口。

### MIT Kerberos配置

需要在客户端进行Kerberos配置。至少需要在`/etc/krb5.conf`文件中的`[realms]`小节中有一个`kdc`条目。还需要增加一个`admin_server`条目，并确保客户端能够通过端口749访问Kerberos管理服务器。

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

`krb5.conf`的完整[文档](http://web.mit.edu/kerberos/krb5-latest/doc/admin/conf_files/kdc_conf.html )由麻省理工学院Kerberos项目托管。如果您使用Kerberos协议的不同实现方式，则需要根据您的环境修改配置。

### Kerberos主体和Keytab文件

连接到openLooKeng协调节点的每个用户都需要一个Kerberos主体。您需要使用[kadmin](http://web.mit.edu/kerberos/krb5-latest/doc/admin/admin_commands/kadmin_local.html )在Kerberos中创建这些用户。

另外，每个用户需要一个[keytab文件](http://web.mit.edu/kerberos/krb5-devel/doc/basic/keytab_def.html )。创建主体后，可使用 **kadmin**创建keytab文件。

```
kadmin
> addprinc -randkey someuser@EXAMPLE.COM
> ktadd -k /home/someuser/someuser.keytab someuser@EXAMPLE.COM
```

注意

运行**ktadd**命令将会使主体的密钥随机化。如果您刚刚创建了主体，这无关紧要。如果主体已经存在，并且现有的用户或服务依赖于使用密码或keytab进行验证，则在运行**ktadd**时需要增加`-norandkey`选项。

### Java加密扩展策略文件

JRE自带策略文件以限制可使用的加密密钥的强度。但默认情况下，Kerberos使用的密钥比策略文件支持的密钥要大。有两种可能的解决方案：

> - 更新JCE策略文件。
> - 配置Kerberos，使用强度降低的密钥。

推荐采用更新JCE策略文件的方案。JCE策略文件可以从Oracle下载。请注意，JCE策略文件因Java主版本而异。例如，Java6的策略文件不能在Java 8中使用。

Java 8策略文件可以从[这里](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html )获取。ZIP归档文件的`README`文件中有安装策略文件的说明。如果要在系统JRE中安装策略文件，则需要管理级别的访问权限。

### TLS使用的Java 密钥库文件

使用Kerberos身份验证时，必须通过https访问openLooKeng协调节点。openLooKeng协调节点使用Java密钥库文件进行TLS配置。该文件可以复制到客户端，并用于其配置。

 

## openLooKeng命令行执行

如果调用使能了Kerberos的CLI，除了连接到不需要Kerberos身份验证的openLooKeng协调节点时所需的选项之外，还需要一些额外的命令行选项。调用CLI的最简单方法是使用包装脚本。

```
#!/bin/bash

./openlk-cli \
  --server https://openlookeng-coordinator.example.com:7778 \
  --krb5-config-path /etc/krb5.conf \
  --krb5-principal someuser@EXAMPLE.COM \
  --krb5-keytab-path /home/someuser/someuser.keytab \
  --krb5-remote-service-name openLooKeng \
  --keystore-path /tmp/openLooKeng.jks \
  --keystore-password password \
  --catalog <catalog> \
  --schema <schema>
```

| 选项                       | 说明                                                  |
| :--------------------------- | :----------------------------------------------------------- |
| `--server`                   | openLooKeng协调节点的地址和端口。  端口必须设置为openLooKeng协调节点侦听HTTPS连接的端口。|
| `--krb5-config-path`         | kerberos配置文件。                                 |
| `--krb5-principal`           | 对协调节点进行身份验证时使用的主体。|
| `--krb5-keytab-path`         | 用于验证`--krb5-principal`指定的主体的keytab的位置。|
| `--krb5-remote-service-name` | openLooKeng协调节点kerberos服务的名称。               |
| `--keystore-path`            | 用于TLS的Java密钥库文件的位置。|
| `--keystore-password`        | 密钥库的密码。必须与创建密钥库时指定的密码一致。|

 

## 异常处理

许多对openLooKeng协调节点进行故障诊断的步骤也同样适用于命令行的故障诊断。

### Kerberos附加调试信息

在启动CLI进程时，可以通过将`-Dsun.security.krb5.debug=true`作为JVM参数传递，为openLooKeng CLI进程启用额外的Kerberos调试信息。这样做要求通过`java`调用CLI JAR，而不是直接运行自执行的JAR。自执行jar文件不能将选项传递给JVM。

```
#!/bin/bash

java \
  -Dsun.security.krb5.debug=true \
  -jar hetu-cli-*-executable.jar \
  --server https://openlookeng-coordinator.example.com:7778 \
  --krb5-config-path /etc/krb5.conf \
  --krb5-principal someuser@EXAMPLE.COM \
  --krb5-keytab-path /home/someuser/someuser.keytab \
  --krb5-remote-service-name openLooKeng \
  --keystore-path /tmp/openLooKeng.jks \
  --keystore-password password \
  --catalog <catalog> \
  --schema <schema>
```

在文档中列出的为openLooKeng协调节点设置Kerberos认证的附加资源对解释Kerberos调试消息可能有帮助。
