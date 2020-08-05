
Java密钥库和信任库
==============================

TLS的Java密钥库文件
--------------------------

使用Kerberos和LDAP身份验证时，必须通过HTTPS访问openLooKeng协调节点。openLooKeng协调节点使用`JavaKeystore<server_java_keystore>`文件进行TLS配置。这些密钥使用`keytool`生成，并存储在Java 密钥库文件中，供openLooKeng协调节点使用。

`keytool`命令行中的别名应与openLooKeng协调节点将要使用的主体匹配。系统将提示您输入姓名。使用将要在证书中使用的Common Name。在这种情况下，它应该是openLooKeng协调节点的非限定主机名。在下面的例子中，在确认信息正确的提示中可以看到这一点：

```
keytool -genkeypair -alias openlookeng -keyalg RSA -keystore keystore.jks
Enter keystore password:
Re-enter new password:
What is your first and last name?
  [Unknown]:  openlookeng-coordinator.example.com
What is the name of your organizational unit?
  [Unknown]:
What is the name of your organization?
  [Unknown]:
What is the name of your City or Locality?
  [Unknown]:
What is the name of your State or Province?
  [Unknown]:
What is the two-letter country code for this unit?
  [Unknown]:
Is CN=openlookeng-coordinator.example.com, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown correct?
  [no]:  yes

Enter key password for <openlookeng>
        (RETURN if same as keystore password):
```

TLS的Java信任库文件
----------------------------

信任库文件包含受信任的TLS/SSL服务器的证书，或证书颁发机构颁发的用于标识服务器的证书。为了确保通过HTTPS访问openLooKeng协调节点的安全性，客户端可以配置信任库。对于openLooKeng命令行要信任openLooKeng协调节点，协调节点的证书必须导入到命令行接口的信任库中。

您可以将证书导入到默认的Java信任库，或者导入到自定义信任库。如果您选择使用默认的Java信任库，则需要小心，因为您可能需要删除您认为不值得信任的CA证书。


可以使用`keytool`将证书导入到信任库。在这个例子中，我们要将`openlookeng_certificate.cer`导入到一个定制的信任库`openlookeng_trust.jks`中，系统会有提示，询问该证书是否可以信任。


``` shell
$ keytool -import -v -trustcacerts -alias openlookeng_trust -file openlookeng_certificate.cer -keystore openlookeng_trust.jks -keypass <truststore_pass>
```

异常处理
---------------

### Java密钥库文件验证

使用[keytool](http://docs.oracle.com/javase/8/docs/technotes/tools/windows/keytool.html)验证密钥库文件的密码并查看其内容。

``` shell
$ keytool -list -v -keystore /etc/openlookeng/openlookeng.jks
```
