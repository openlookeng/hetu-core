
Hazelcast安全
===================================
Hazelcast是内嵌在openLooKeng进程中，在openLooKeng中使用Hazelcast时，为了保障Hazelcast的客户端与服务端，以及各个服务端成员之间的通信安全，
建议开启Hazelcast的认证与SSL/TLS通道加密。

## Hazelcast认证
Hazelcast当前只支持Kerberos认证。由于Hazelcast是由state-store模块调用，因此开启Hazelcast认证，需先启用state-store。


state-store启用后，在state-store的配置文件state-store.properties中增加如下配置：

> ```properties
> hazelcast.kerberos.enable=true
> hazelcast.kerberos.login.context.name=Hazelcast
> hazelcast.kerberos.service.principal=openlookeng
> hazelcast.kerberos.krb5.conf=/etc/krb5.conf
> hazelcast.kerberos.auth.login.config=/etc/jaas.conf
> ```

| 属性                                             | 描述                                                  |
| :--------------------------------------------------- | :----------------------------------------------------------- |
| `hazelcast.kerberos.enable` | 为Hazelcast开启Kerberos认证功能。默认设置为`false`。|
| `hazelcast.kerberos.login.context.name` | 登陆Kerberos的context名。|
| `hazelcast.kerberos.service.principal` | Hazelcast的Kerberos服务主体名。|
| `hazelcast.kerberos.krb5.conf` | kerberos配置文件所在的位置。             |    
| `hazelcast.kerberos.auth.login.config` |登陆Kerberos配置文件所在的位置。|


登陆Kerberos配置文件jaas.conf格式如下，配置时需先在Kerberos创建机机用户，并将创建的用户的principal，keytab按照如下格式配置

> ```properties
> Hazelcast {
> com.sun.security.auth.module.Krb5LoginModule required
> useKeyTab=true
> principal="openlookeng"
> keyTab="/etc/openlookeng.keytab"
> useTicketCache=false
> storeKey=true;
> };
> ```

| 属性                                             | 描述                                                  |
| :--------------------------------------------------- | :----------------------------------------------------------- |
| `principal` | 登陆Kerberos的主体名。|
| `keyTab` | 登陆Kerberos主体进行身份验证的keytab文件的位置。|


注意：
所有节点的配置文件相同，包括机机用户的principal，ketTab。

## Hazelcast SSL/TLS

在`state-store.properties`文件中进行SSL/TLS配置。所有需要使用state-store的节点上均采用相同的配置。开启SSL/TLS后，未配置SSL/TLS或配置错
误的节点将无法与其他节点进行通信。

为了Hazelcast通信启用SSL/TLS功能，需要执行以下步骤：
1.  生成Java密钥库文件。可以使用每台主机的fully-qualified主机名为每个节点创建唯一的证书，创建时包含所有主机的所有公钥的密钥库，并为客户端
指定密钥库。在大多数情况下，使用通配符进行证书的创建更加方便，如下所示：

    > ``` 
    > keytool -genkeypair -alias openLooKeng -keyalg EC -keysize 256 -validity 365 -keystore keystore.jks -storepass <password>
    >     What is your first and last name?
    >       [Unknown]:  *.example.com
    >     What is the name of your organizational unit?
    >       [Unknown]:  
    >     What is the name of your organization?
    >       [Unknown]:  
    >     What is the name of your City or Locality?
    >       [Unknown]:  
    >     What is the name of your State or Province?
    >       [Unknown]:  
    >     What is the two-letter country code for this unit?
    >       [Unknown]:  
    >     Is CN=*.example.com, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown correct?
    >       [no]:  yes
    >     
    >     Enter key password for <openLooKeng>
    >     	(RETURN if same as keystore password):
    > ```
    > 

2.  将Java keystore文件分发到其他节点上。
3.  在`state-store.properties`配置启用SSL/TLS。

    > ```
    > hazelcast.ssl.enabled=true
    > hazelcast.ssl.keystore.path=<keystore path>
    > hazelcast.ssl.keystore.password=<keystore pasword>
    > hazelcast.ssl.cipher.suites=<cipher suite list>
    > hazelcast.ssl.protocols=<ssl protocol list>
    > ```
    > 推荐的加密套件为TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
    >
    > 推荐的SSL协议为TLS1.2或TLS1.3

