
Hazelcast Security
===================================

Hazelcast is embedded in the openLooKeng. Hazelcast is used in openLooKeng to ensure the communication is secured between the client and the server of hazelcast, as well as between the members of each server.
It is recommended to enable hazelcast authentication and SSL/TLS channel encryption.

## Hazelcast Authentication

Only Kerberos authentication is supported by Hazelcast. Because the state-store module uses hazelcast, if the user wants to use Hazelcast authentication, the state-store must first be enabled.

When state-store enabled, add the following configuration in the state-store.properties:

> ```properties
> hazelcast.kerberos.enable=true
> hazelcast.kerberos.login.context.name=Hazelcast
> hazelcast.kerberos.service.principal=openlookeng
> hazelcast.kerberos.krb5.conf=/etc/krb5.conf
> hazelcast.kerberos.auth.login.config=/etc/jaas.conf
> ```

| Property                                             | Description                                                  |
| :--------------------------------------------------- | :----------------------------------------------------------- |
| `hazelcast.kerberos.enable` | Enable Hazelcast authentication, the default value is `false`.|
| `hazelcast.kerberos.login.context.name` | The context name to login the kerberos.|
| `hazelcast.kerberos.service.principal` | The service principal name of kerberos|
| `hazelcast.kerberos.krb5.conf` | The location of the Kerberos configuration file.|    
| `hazelcast.kerberos.auth.login.config` |The location of the configuration file to login the kerberos|

The format of the configuration file `jass.conf` to login the kerberos is as follows. The user must first create a kerberos principal and configure the principal and keytab.

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

| Property    | Description                                                  |
| :---------- | :----------------------------------------------------------- |
| `principal` | The principal name to login the Kerberos                     |
| `keyTab`    | The location of the keytab that can be used to authenticate the Kerberos principal. |

**Note:**

All of the nodes must use the same configuration, including the kerberos principal and keytab.


## Hazelcast SSL/TLS

SSL/TLS is configured in the `state-store.properties`. The same configuration is used on all nodes that need to use state store. After SSL/TLS is enabled, nodes that have not been configured SSL/ TLS is or configured incorrectly, will not be able to communicate with other nodes.

To enable SSL/TLS for Hazelcast, do the following:

1. Generate Java keystore file. You can construct a unique certificate for each node using the fully-qualified host name of each host, which contains the key store of all public keys of all hosts and specifies the keystore.
   In most cases, it is more convenient to use wildcards to create certificates, as follows:

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
   
2.  Distribute the Java keystore file to other nodes.
3.  Enable the SSL/TLS of Hazelcast in `state-store.properties`.

    > ```
    > hazelcast.ssl.enabled=true
    > hazelcast.ssl.keystore.path=<keystore path>
    > hazelcast.ssl.keystore.password=<keystore pasword>
    > hazelcast.ssl.cipher.suites=<cipher suite list>
    > hazelcast.ssl.protocols=<ssl protocol list>
    > ```
    > Suggested cipher suite is TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256.
    >
    > Suggested ssl protocol is TLS1.2 or TLS1.3.

