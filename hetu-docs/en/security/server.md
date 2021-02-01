
Coordinator Kerberos Authentication
===================================

The openLooKeng coordinator can be configured to enable Kerberos authentication over HTTPS for clients, such as the openLooKeng CLI, or the JDBC and ODBC drivers.

 

To enable Kerberos authentication for openLooKeng , configuration changes are made on the openLooKeng  coordinator. No changes are required to the worker configuration; the worker nodes will continue to connect to the coordinator over unauthenticated HTTP. However, if you want to secure the communication between openLooKeng  nodes with SSL/TLS, configure Secure Internal Communication.


Environment Configuration
-------------------------

### Kerberos Services

You will need a Kerberos KDC running on a node that the openLooKeng  coordinator can reach over the network. The KDC is responsible for authenticating principals and issuing session keys that can be used with Kerberos-enabled services. KDCs typically run on port 88, which is the IANA-assigned port for Kerberos.

 MIT Kerberos Configuration

Kerberos needs to be configured on the openLooKeng  coordinator. At a minimum, there needs to be a `kdc` entry in the `[realms]` section of the `/etc/krb5.conf` file. You may also want to include an `admin_server` entry and ensure that the openLooKeng  coordinator can reach the Kerberos admin server on port 749.

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

The complete [documentation](http://web.mit.edu/kerberos/krb5-latest/doc/admin/conf_files/kdc_conf.html) for `krb5.conf` is hosted by the MIT Kerberos Project. If you are using a different implementation of the Kerberos protocol, you will need to adapt the configuration to your environment.

### Kerberos Principals and Keytab Files

The openLooKeng  coordinator needs a Kerberos principal, as do users who are going to connect to the openLooKeng  coordinator. You will need to create these users in Kerberos using [kadmin](http://web.mit.edu/kerberos/krb5-latest/doc/admin/admin_commands/kadmin_local.html).

In addition, the openLooKeng  coordinator needs a [keytab file](http://web.mit.edu/kerberos/krb5-devel/doc/basic/keytab_def.html). After you create the principal, you can create the keytab file using **kadmin**

```
kadmin
> addprinc -randkey openlookeng@EXAMPLE.COM
> addprinc -randkey openlookeng/openlookeng-coordinator.example.com@EXAMPLE.COM
> ktadd -k /etc/openlookeng/openlookeng.keytab openlookeng@EXAMPLE.COM
> ktadd -k /etc/openlookeng/openlookeng.keytab openlookeng/openlookeng-coordinator.example.com@EXAMPLE.COM
```

**Note**

*Running **ktadd** randomizes the principal’s keys. If you have just created the principal, this does not matter. If the principal already exists, and if existing users or services rely on being able to authenticate using a password or a keytab, use the `-norandkey` option to **ktadd**.*

### Java Cryptography Extension Policy Files

The Java Runtime Environment is shipped with policy files that limit the strength of the cryptographic keys that can be used. Kerberos, by default, uses keys that are larger than those supported by the included policy files. There are two possible solutions to the problem:

> - Update the JCE policy files.
> - Configure Kerberos to use reduced-strength keys.

Of the two options, updating the JCE policy files is recommended. The JCE policy files can be downloaded from Oracle. Note that the JCE policy files vary based on the major version of Java you are running. Java 6 policy files will not work with Java 8, for example.

The Java 8 policy files are available [here](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html). Instructions for installing the policy files are included in a `README` file in the ZIP archive. You will need administrative access to install the policy files if you are installing them in a system JRE.

### Java Keystore File for TLS

When using Kerberos authentication, access to the openLooKeng  coordinator should be through HTTPS. You can do it by creating a Java Keystore File for TLS on the coordinator.

 

## System Access Control Plugin

A openLooKeng  coordinator with Kerberos enabled will probably need a System Access Control plugin to achieve the desired level of security.

 

## openLooKeng  Coordinator Node Configuration

You must make the above changes to the environment prior to configuring the openLooKeng  coordinator to use Kerberos authentication and HTTPS. After making the following environment changes, you can make the changes to the openLooKeng  configuration files.

- Kerberos Services
- MIT Kerberos Configuration
- Kerberos Principals and Keytab Files
- Java Keystore File for TLS
- System Access Control Plugin

### config.properties

Kerberos authentication is configured in the coordinator node’s `config.properties` file. The entries that need to be added are listed below.

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

| Property                                               | Description                                                  |
| :----------------------------------------------------- | :----------------------------------------------------------- |
| `http-server.authentication.type`                      | Authentication type for the openLooKeng  coordinator. Must be set to `KERBEROS`. |
| `http.server.authentication.krb5.service-name`         | The Kerberos service name for the openLooKeng  coordinator. Must match the Kerberos principal. |
| `http.server.authentication.krb5.principal-hostname`   | The Kerberos hostname for the openLooKeng  coordinator. Must match the Kerberos principal. This parameter is optional. If included, openLooKeng  will use this value in the host part of the Kerberos principal instead of the machine’s hostname. |
| `http.server.authentication.krb5.keytab`               | The location of the keytab that can be used to authenticate the Kerberos principal. |
| `http.authentication.krb5.config`                      | The location of the Kerberos configuration file.          |
| `http-server.https.enabled`                            | Enables HTTPS access for the openLooKeng  coordinator. Should be set to `true`. |
| `http-server.https.port`                               | HTTPS server port.                                         |
| `http-server.https.keystore.path`                      | The location of the Java Keystore file that will be used to secure TLS. |
| `http-server.https.keystore.key`                       | The password for the keystore. This must match the password you specified when creating the keystore. |
| `http-server.authentication.krb5.user-mapping.pattern` | Regex to match against user. If matched, user will be replaced with first regex group. If not matched, authentication is denied. Default is `(.*)`. |
| `http-server.authentication.krb5.user-mapping.file`    | JSON file containing rules for mapping user. See [Authentication User Mapping](./user-mapping.md) for more information. |

Note

`http-server.authentication.krb5.user-mapping.pattern` and `http-server.authentication.krb5.user-mapping.file` can not both be set.

Monitor CPU usage on the openLooKeng coordinator after enabling HTTPS. Java prefers the more CPU-intensive cipher suites if you allow it to choose from a big list. If the CPU usage is unacceptably high after enabling HTTPS, you can configure Java to use specific cipher suites by setting the `http-server.https.included-cipher` property to only allow cheap ciphers. Non forward secrecy (FS) ciphers are disabled by default. As a result, if you want to choose non FS ciphers, you need to set the `http-server.https.excluded-cipher` property to an empty list in order to override the default exclusions.

```properties
http-server.https.included-cipher=TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA256
http-server.https.excluded-cipher=
```

The Java documentation lists the [supported cipher suites](http://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html#SupportedCipherSuites).

### access-controls.properties

At a minimum, an `access-control.properties` file must contain an `access-control.name` property. All other configuration is specific for the implementation being configured. See System Access Control for details.

## Troubleshooting

Getting Kerberos authentication working can be challenging. You can independently verify some of the configuration outside of openLooKeng  to help narrow your focus when trying to solve a problem.

### Kerberos Verification

Ensure that you can connect to the KDC from the openLooKeng  coordinator using **telnet**.

```
$ telnet kdc.example.com 88
```

Verify that the keytab file can be used to successfully obtain a ticket using [kinit](http://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html) and [klist](http://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/klist.html)

```
$ kinit -kt /etc/openlookeng/openlookeng.keytab openlookeng@EXAMPLE.COM
$ klist
```

### Java Keystore File Verification

Verify the password for a keystore file and view its contents using [Java Keystore File Verification](tls.md#java-keystore-file-verification).

### Additional Kerberos Debugging Information

You can enable additional Kerberos debugging information for the openLooKeng  coordinator process by adding the following lines to the openLooKeng  `jvm.config` file

```properties
-Dsun.security.krb5.debug=true
-Dlog.enable-console=true
```

`-Dsun.security.krb5.debug=true` enables Kerberos debugging output from the JRE Kerberos libraries. The debugging output goes to `stdout`, which openLooKeng  redirects to the logging system. `-Dlog.enable-console=true` enables output to `stdout` to appear in the logs.

The amount and usefulness of the information the Kerberos debugging output sends to the logs varies depending on where the authentication is failing. Exception messages and stack traces can also provide useful clues about the nature of the problem.



### Additional resources

[Common Kerberos Error Messages (A-M)](http://docs.oracle.com/cd/E19253-01/816-4557/trouble-6/index.html)

[Common Kerberos Error Messages (N-Z)](http://docs.oracle.com/cd/E19253-01/816-4557/trouble-27/index.html)

[MIT Kerberos Documentation: Troubleshooting](http://web.mit.edu/kerberos/krb5-latest/doc/admin/troubleshoot.html)