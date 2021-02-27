
LDAP Authentication
===================

openLooKeng can be configured to enable frontend LDAP authentication over HTTPS for clients, such as the `cli_ldap`, or the JDBC and ODBC drivers. At present only simple LDAP authentication mechanism involving username and password is supported. The openLooKeng client sends a username and password to the coordinator and coordinator validates these credentials using an external LDAP service.

To enable LDAP authentication for openLooKeng, configuration changes are made on the openLooKeng coordinator. No changes are required to the worker configuration; only the communication from the clients to the coordinator is authenticated. However, if you want to secure the communication between openLooKeng nodes with SSL/TLS configure `/security/internal-communication`.

openLooKeng Server Configuration
-------------------------

### Environment Configuration

#### Secure LDAP

openLooKeng requires Secure LDAP (LDAPS), so make sure you have TLS enabled on your LDAP server.

#### TLS Configuration on openLooKeng Coordinator

You need to import the LDAP server\'s TLS certificate to the default Java truststore of the openLooKeng coordinator to secure TLS connection. You can use the following example `keytool` command to import the certificate `ldap_server.crt`, to the truststore on the coordinator.

``` shell
$ keytool -import -keystore <JAVA_HOME>/jre/lib/security/cacerts -trustcacerts -alias ldap_server -file ldap_server.crt
```

In addition to this, access to the openLooKeng coordinator should be through HTTPS. You can do it by creating a
`server_java_keystore` on the coordinator.

### openLooKeng Coordinator Node Configuration

You must make the following changes to the environment prior to configuring the openLooKeng coordinator to use LDAP authentication and HTTPS.

> -   `ldap_server`
> -   `server_java_keystore`

You also need to make changes to the openLooKeng configuration files. LDAP authentication is configured on the coordinator in two parts. The first part is to enable HTTPS support and password authentication in the
coordinator\'s `config.properties` file. The second part is to configure LDAP as the password authenticator plugin.

#### Server Config Properties

The following is an example of the required properties that need to be added to the coordinator\'s `config.properties` file:

``` properties
http-server.authentication.type=PASSWORD

http-server.https.enabled=true
http-server.https.port=8443

http-server.https.keystore.path=/etc/openLooKeng.jks
http-server.https.keystore.key=keystore_password
```

 

| Property                                                   | Description                                                  |
| :--------------------------------------------------------- | :----------------------------------------------------------- |
| `http-server.authentication.type`                          | Enable password authentication for the openLooKeng coordinator. Must be set to `PASSWORD`. |
| `http-server.https.enabled`                                | Enables HTTPS access for the openLooKeng coordinator. Should be set to `true`. Default value is `false`. |
| `http-server.https.port`                                   | HTTPS server port.                                           |
| `http-server.https.keystore.path`                          | The location of the Java Keystore file that will be used to secure TLS. |
| `http-server.https.keystore.key`                           | The password for the keystore. This must match the password you specified when creating the keystore. |
| `http-server.authentication.password.user-mapping.pattern` | Regex to match against user. If matched, user will be replaced with first regex group. If not matched, authentication is denied. Default is `(.*)`. |
| `http-server.authentication.password.user-mapping.file`    | JSON file containing rules for mapping user. See [Authentication User Mapping](./user-mapping.md) for more information. |

Note

`http-server.authentication.password.user-mapping.pattern` and `http-server.authentication.password.user-mapping.file` can not both be set. 

#### Password Authenticator Configuration

Password authentication needs to be configured to use LDAP. Create an `etc/password-authenticator.properties` file on the coordinator.
Example:

``` properties
password-authenticator.name=ldap
ldap.url=ldaps://ldap-server:636
ldap.user-bind-pattern=<Refer below for usage>
```

| Property                 | Description                                                  |
| :----------------------- | :----------------------------------------------------------- |
| `ldap.url`               | The url to the LDAP server. The url scheme must be `ldaps://` since openLooKeng allows only Secure LDAP. |
| `ldap.user-bind-pattern` | This property can be used to specify the LDAP user bind string for password authentication. This property must contain the pattern `${USER}` which will be replaced by the actual username during the password authentication. Example: `${USER}@corp.example.com`. |


Based on the LDAP server implementation type, the property `ldap.user-bind-pattern` can be used as described below.

##### Active Directory

``` properties
ldap.user-bind-pattern=${USER}@<domain_name_of_the_server>
```

Example:

``` properties
ldap.user-bind-pattern=${USER}@corp.example.com
```

##### OpenLDAP

``` properties
ldap.user-bind-pattern=uid=${USER},<distinguished_name_of_the_user>
```

Example:

``` properties
ldap.user-bind-pattern=uid=${USER},OU=America,DC=corp,DC=example,DC=com
```

#### Authorization based on LDAP Group Membership

You can further restrict the set of users allowed to connect to the openLooKeng coordinator based on their group membership by setting the optional `ldap.group-auth-pattern` and `ldap.user-base-dn` properties in addition to the basic LDAP authentication properties.

| Property                  | Description                                                  |
| :------------------------ | :----------------------------------------------------------- |
| `ldap.user-base-dn`       | The base LDAP distinguished name for the user who tries to connect to the server. Example: `OU=America,DC=corp,DC=example,DC=com` |
| `ldap.group-auth-pattern` | This property is used to specify the LDAP query for the LDAP group membership authorization. This query will be executed against the LDAP server and if successful, the user will be authorized. This property must contain a pattern `${USER}` which will be replaced by the actual username in the group authorization search query. See samples below. |

 Based on the LDAP server implementation type, the property `ldap.group-auth-pattern` can be used as described below.

##### Active Directory

``` 
ldap.group-auth-pattern=(&(objectClass=<objectclass_of_user>)(sAMAccountName=${USER})(memberof=<dn_of_the_authorized_group>))
```

Example:

``` 
ldap.group-auth-pattern=(&(objectClass=person)(sAMAccountName=${USER})(memberof=CN=AuthorizedGroup,OU=Asia,DC=corp,DC=example,DC=com))
```

##### OpenLDAP

``` 
ldap.group-auth-pattern=(&(objectClass=<objectclass_of_user>)(uid=${USER})(memberof=<dn_of_the_authorized_group>))
```

Example:

``` 
ldap.group-auth-pattern=(&(objectClass=inetOrgPerson)(uid=${USER})(memberof=CN=AuthorizedGroup,OU=Asia,DC=corp,DC=example,DC=com))
```

For OpenLDAP, for this query to work, make sure you enable the `memberOf` [overlay](http://www.openldap.org/doc/admin24/overlays.html).

You can also use this property for scenarios where you want to authorize a user based on complex group authorization search queries. For example, if you want to authorize a user belonging to any one of multiple groups (in OpenLDAP), this property may be set as follows:

``` 
ldap.group-auth-pattern=(&(|(memberOf=CN=normal_group,DC=corp,DC=com)(memberOf=CN=another_group,DC=com))(objectClass=inetOrgPerson)(uid=${USER}))
```

openLooKeng CLI
--------

### Environment Configuration

#### TLS Configuration

Access to the openLooKeng coordinator should be through HTTPS when using LDAP authentication. The openLooKeng CLI can use either a `Java Keystore <server_java_keystore>` file or `Java Truststore <cli_java_truststore>` for its TLS configuration.

If you are using keystore file, it can be copied to the client machine and used for its TLS configuration. If you are using truststore, you can either use default java truststores or create a custom truststore on the CLI. We do not recommend using self-signed certificates in production.

### openLooKeng CLI Execution

In addition to the options that are required when connecting to a openLooKeng coordinator that does not require LDAP authentication, invoking the CLI with LDAP support enabled requires a number of additional command line options. You can either use `--keystore-*` or `--truststore-*`properties to secure TLS connection. The simplest way to invoke the CLI is with a wrapper script.

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

| Option                  | Description                                                  |
| :---------------------- | :----------------------------------------------------------- |
| `--server`              | The address and port of the openLooKeng coordinator.  The port must be set to the port the openLooKeng coordinator is listening for HTTPS connections on. openLooKeng CLI does not support using `http` scheme for the url when using LDAP authentication. |
| `--keystore-path`       | The location of the Java Keystore file that will be used to secure TLS. |
| `--keystore-password`   | The password for the keystore. This must match the password you specified when creating the keystore. |
| `--truststore-path`     | The location of the Java Truststore file that will be used to secure TLS. |
| `--truststore-password` | The password for the truststore. This must match the password you specified when creating the truststore. |
| `--user`                | The LDAP username. For Active Directory this should be your `sAMAccountName` and for OpenLDAP this should be the `uid` of the user. This is the username which will be used to replace the `${USER}` placeholder pattern in the properties specified in `config.properties`. |
| `--password`            | Prompts for a password for the `user`.                       |

Troubleshooting
---------------

### Java Keystore File Verification

Verify the password for a keystore file and view its contents using [Java Keystore File Verification](tls.md#java-keystore-file-verification).

### SSL Debugging for openLooKeng CLI

If you encounter any SSL related errors when running openLooKeng CLI, you can run CLI 

using `-Djavax.net.debug=ssl` parameter for debugging. You should use the openLooKeng CLI executable jar to enable this. Eg:

``` shell
java -Djavax.net.debug=ssl \
-jar \
hetu-cli-<version>-executable.jar \
--server https://coordinator:8443 \
<other_cli_arguments>
```

#### Common SSL errors

##### java.security.cert.CertificateException: No subject alternative names present

This error is seen when the openLooKeng coordinator's certificate is invalid and does not have the IP you provide in the `--server` argument of the CLI. You will have to regenerate the coordinator\'s SSL certificate with the appropriate `SAN (Subject Alternative Name)` added.

Adding a SAN to this certificate is required in cases where `https://` uses IP address in the URL rather than the domain contained in the coordinator\'s certificate, and the certificate does not contain the `SAN (Subject Alternative Name)` parameter with the matching IP address as an alternative attribute.

#### Authentication or SSL errors with JDK Upgrade

Starting with the JDK 8u181 release, to improve the robustness of LDAPS (secure LDAP over TLS) connections, endpoint identification algorithms have been enabled by default. See release notes
[here](https://www.oracle.com/technetwork/java/javase/8u181-relnotes-4479407.html#JDK-8200666.).
The same LDAP server certificate on the openLooKeng coordinator (running on JDK version \>= 8u181) that was previously able to successfully connect toan LDAPS server may now fail with the below error:

``` 
javax.naming.CommunicationException: simple bind failed: ldapserver:636
[Root exception is javax.net.ssl.SSLHandshakeException: java.security.cert.CertificateException: No subject alternative DNS name matching ldapserver found.]
```

If you want to temporarily disable endpoint identification you can add the 

property`-Dcom.sun.jndi.ldap.object.disableEndpointIdentification=true` to openLooKeng\'s `jvm.config` file. However, in a production environment, we suggest fixing the issue by regenerating the LDAP server certificate so that the certificate `SAN (Subject Alternative Name)` or certificate subject name matches the LDAP server.