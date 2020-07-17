
CLI Kerberos Authentication
===========================

The openLooKeng Command Line Interface can connect to a openLooKeng coordinator that has Kerberos authentication enabled.

 

## Environment Configuration

### Kerberos Services

You will need a Kerberos KDC running on a node that the client can reach over the network. The KDC is responsible for authenticating principals and issuing session keys that can be used with Kerberos-enabled services. KDCs typically run on port 88, which is the IANA-assigned port for Kerberos.

### MIT Kerberos Configuration

Kerberos needs to be configured on the client. At a minimum, there needs to be a `kdc` entry in the `[realms]` section of the `/etc/krb5.conf` file. You may also want to include an `admin_server` entry and ensure that the client can reach the Kerberos admin server on port 749.

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

Each user who connects to the openLooKeng coordinator needs a Kerberos principal. You will need to create these users in Kerberos using [kadmin](http://web.mit.edu/kerberos/krb5-latest/doc/admin/admin_commands/kadmin_local.html).

Additionally, each user needs a [keytab file](http://web.mit.edu/kerberos/krb5-devel/doc/basic/keytab_def.html). The keytab file can be created using **kadmin** after you create the principal.

```
kadmin
> addprinc -randkey someuser@EXAMPLE.COM
> ktadd -k /home/someuser/someuser.keytab someuser@EXAMPLE.COM
```

Note

Running **ktadd** randomizes the principal’s keys. If you have just created the principal, this does not matter. If the principal already exists, and if existing users or services rely on being able to authenticate using a password or a keytab, use the `-norandkey` option to **ktadd**.

### Java Cryptography Extension Policy Files

The Java Runtime Environment is shipped with policy files that limit the strength of the cryptographic keys that can be used. Kerberos, by default, uses keys that are larger than those supported by the included policy files. There are two possible solutions to the problem:

> - Update the JCE policy files.
> - Configure Kerberos to use reduced-strength keys.

Of the two options, updating the JCE policy files is recommended. The JCE policy files can be downloaded from Oracle. Note that the JCE policy files vary based on the major version of Java you are running. Java 6 policy files will not work with Java 8, for example.

The Java 8 policy files are available [here](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html). Instructions for installing the policy files are included in a `README` file in the ZIP archive. You will need administrative access to install the policy files if you are installing them in a system JRE.

### Java Keystore File for TLS

Access to the openLooKeng coordinator must be through https when using Kerberos authentication. The openLooKeng coordinator uses a Java Keystore file for its TLS configuration. This file can be copied to the client machine and used for its configuration.

 

## openLooKeng CLI execution

In addition to the options that are required when connecting to a openLooKeng coordinator that does not require Kerberos authentication, invoking the CLI with Kerberos support enabled requires a number of additional command line options. The simplest way to invoke the CLI is with a wrapper script.

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

| Option                       | Description                                                  |
| :--------------------------- | :----------------------------------------------------------- |
| `--server`                   | The address and port of the openLooKeng coordinator.  The port must be set to the port the openLooKeng coordinator is listening for HTTPS connections on. |
| `--krb5-config-path`         | Kerberos configuration file.                                 |
| `--krb5-principal`           | The principal to use when authenticating to the coordinator. |
| `--krb5-keytab-path`         | The location of the the keytab that can be used to authenticate the principal specified by `--krb5-principal` |
| `--krb5-remote-service-name` | openLooKeng coordinator Kerberos service name.               |
| `--keystore-path`            | The location of the Java Keystore file that will be used to secure TLS. |
| `--keystore-password`        | The password for the keystore. This must match the password you specified when creating the keystore. |

 

## Troubleshooting

Many of the same steps that can be used when troubleshooting the openLooKeng coordinator apply to troubleshooting the CLI.

### Additional Kerberos Debugging Information

You can enable additional Kerberos debugging information for the openLooKeng CLI process by passing `-Dsun.security.krb5.debug=true` as a JVM argument when starting the CLI process. Doing so requires invoking the CLI JAR via `java` instead of running the self-executable JAR directly. The self-executable jar file cannot pass the option to the JVM.

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

The additional resources listed in the documentation for setting up Kerberos authentication for the openLooKeng coordinator may be of help when interpreting the Kerberos debugging messages.