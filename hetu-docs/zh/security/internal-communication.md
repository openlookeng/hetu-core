
内部通信安全
=============================

openLooKeng集群可以通过配置使用安全通信。  可使用SSL/TLS确保openLooKeng节点间的通信安全。

内部SSL/TLS配置
------------------------------

在`config.properties`文件中进行SSL/TLS配置。使用相同的属性对工作（worker)节点和协调(coordinator)节点上的SSL/TLS进行配置。集群中每个节点都需要进行配置。未配置SSL/TLS或配置错误的节点将无法与集群中的其他节点进行通信。


为openLooKeng内部通信启用SSL/TLS功能，执行以下步骤：

1.  禁用HTTP端点。

    > ``` properties
    > http-server.http.enabled=false
    > ```
    > 
    > 
    > **警告**
    > 
    > 可以在不禁用HTTP的情况下，开启HTTPS。但在大多数情况下，这会有安全风险。
    > 如果您确定要使用此配置，则应考虑使用防火墙来确保HTTP端点不被非法的主机访问。
    > 
    
2.  配置集群使用集群节点的FQDN（全量域名）进行通信。可通过以下两种方式实现：
    
    -   如果DNS服务配置正常，可以让节点使用从系统配置获得的主机名（`hostname --fqdn`）向协调节点介绍自己。
    
        ``` properties
        node.internal-address-source=FQDN
        ```
    
    -   手动指定每个节点的完全限定主机名。每台主机的主机名应该不同。主机应该在同一个域中，以便创建正确的SSL/TLS证书。如：`coordinator.example.com`, `worker1.example.com`, `worker2.example.com`.
        
        ``` properties
        node.internal-address=<node fqdn>
        ```
    
3.  生成Java 密钥库文件。每个openLooKeng节点必须能够连接到同一集群中的任何其他节点。可以使用每台主机的完全限定主机名为每个节点创建唯一的证书，创建包含所有主机的所有公钥的密钥库，并为客户端指定密钥库（见下面的步骤[8](#step08)）。在大多数情况下，在证书中使用通配符会更简单，如下所示。
    
    > ``` shell
    > keytool -genkeypair -alias openLooKeng -keyalg RSA -keystore keystore.jks -keysize 2048
    > Enter keystore password:
    > Re-enter new password:
    > What is your first and last name?
    > [Unknown]:  *.example.com
    > What is the name of your organizational unit?
    >   [Unknown]:
    > What is the name of your organization?
    >   [Unknown]:
    > What is the name of your City or Locality?
    >   [Unknown]:
    > What is the name of your State or Province?
    >   [Unknown]:
    > What is the two-letter country code for this unit?
    >   [Unknown]:
    > Is CN=*.example.com, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown correct?
    >   [no]:  yes
    >
    > Enter key password for <openLooKeng>
    >         (RETURN if same as keystore password):
    > ```
	建议keysize不小于2048
4.  为openLooKeng集群分发Java密钥库文件。

5.  启用HTTPS端点。

    > ``` properties
    > http-server.https.enabled=true
    > http-server.https.port=<https port>
    > http-server.https.keystore.path=<keystore path>
    > http-server.https.keystore.key=<keystore password>
    > ```

6.  将discovery URI修改为HTTPS地址。

    > ``` properties
    > discovery.uri=https://<coordinator fqdn>:<https port>
    > ```

7.  配置内部通信需要使用HTTPS协议。

    > ``` properties
    > internal-communication.https.required=true
    > ```

8.  <a name = "step08"></a>配置内部通信使用Java密钥库文件。

    > ``` properties
    > internal-communication.https.keystore.path=<keystore path>
    > internal-communication.https.keystore.key=<keystore password>
    > ```

### 使用Kerberos进行内部SSL/TLS通信

如果启用了[Kerberos](server.md)认证，除了配置SSL/TLS属性外，还需要配置有效的Kerberos凭证用于内部通信。

> ``` properties
> internal-communication.kerberos.enabled=true
> ```


**注意**

*用于内部Kerberos认证的服务名和keytab文件来自于服务器Kerberos认证属性，这些属性分别在`Kerberos</security/server>`, `http.server.authentication.krb5.service-name` 和 `http.server.authentication.krb5.keytab`文档中设置。在worker节点上也必须完成Kerberos设置。用于内部通信的Kerberos主体是通过`http.server.authentication.krb5.service-name`后面追加主机名（运行openLooKeng服务的节点的）和默认域（Kerberos配置）来构建的。*


开启SSL/TLS后性能
--------------------------------

启用加密会影响性能。性能下降可能因环境、查询数和并发度而异。

对于不需要在openLooKeng节点之间传输太多数据的查询（例如`SELECT count(*） FROM table`)，对性能影响可以忽略。

但是，对于需要在节点之间传输大量数据的CPU密集型查询（例如需要重分区的分布式联接、聚合和窗口函数），性能影响可能相当大。
根据网络流量和CPU利用率的不同，可能会有10%到100%以上的减速。

高级性能调优
---------------------------

在某些情况下，改变随机数的来源可显著提高性能。

TLS加密默认使用系统设备`/dev/urandom`作为熵源。这种设备吞吐量有限，在具有高网络带宽（例如InfiniBand）的环境中，可能会成为瓶颈。在这种情况下，建议尝试将随机数生成器算法切换为`SHA1PRNG`。方法是在协调节点和所有worker节点通过`config.properties`中的`http-server.https.secure-random-algorithm`属性进行设置。

> ``` properties
> http-server.https.secure-random-algorithm=SHA1PRNG
> ```

请注意，此算法从阻塞`/dev/random`设备获取初始种子。对于没有足够熵来生成`SHAPRNG`算法的环境，可以通过在`jvm.config`中添加`java.security.egd`属性，将源改为`/dev/urandom`：

> ``` properties
> -Djava.security.egd=file:/dev/urandom
> ```
