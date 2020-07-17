
Java Keystores and Truststores
==============================

Java Keystore File for TLS
--------------------------

Access to the openLooKeng coordinator must be through HTTPS when using Kerberos and LDAP authentication. The openLooKeng coordinator uses a `Java Keystore <server_java_keystore>` file for its TLS configuration. These keys are generated using `keytool` and stored in a Java Keystore file for the openLooKeng coordinator.

The alias in the `keytool` command line should match the principal that the openLooKeng coordinator will use. You\'ll be prompted for the first and last name. Use the Common Name that will be used in the certificate. In this case, it should be the unqualified hostname of the openLooKeng coordinator. In the following example, you can see this in the prompt that confirms the information is correct:

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

Java Truststore File for TLS
----------------------------

Truststore files contain certificates of trusted TLS/SSL servers, or of Certificate Authorities trusted to identify servers. For securing access to the openLooKeng coordinator through HTTPS the clients can configure truststores. For the openLooKeng CLI to trust the openLooKeng coordinator, the coordinator\'s certificate must be imported to the CLI\'s truststore.

You can either import the certificate to the default Java truststore, or to a custom truststore. You should be careful if you choose to use the default one, since you may need to remove the certificates of CAs you do
not deem trustworthy.

You can use `keytool` to import the certificate to the truststore. In the example, we are going to import `openlookeng_certificate.cer` to a custom truststore `openlookeng_trust.jks`, and you
will get a prompt asking if the certificate can be trusted or not.

``` shell
$ keytool -import -v -trustcacerts -alias openlookeng_trust -file openlookeng_certificate.cer -keystore openlookeng_trust.jks -keypass <truststore_pass>
```

Troubleshooting
---------------

### Java Keystore File Verification

Verify the password for a keystore file and view its contents using [keytool](http://docs.oracle.com/javase/8/docs/technotes/tools/windows/keytool.html).

``` shell
$ keytool -list -v -keystore /etc/openlookeng/openlookeng.jks
```
