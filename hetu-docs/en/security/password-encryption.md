
Password Encryption
===================

Overview
-------------------------
openLooKeng manages configuration details in properties files of catalogs. These files may need to include values such as usernames, passwords and other strings, the password often required to be kept secret, that can't be stored as plaintext.
Here is a typical configuration file of a MySQL connector:
```
connector.name=mysql
connection-url=jdbc:mysql://localhost:3306
connection-user=root
connection-password=123456
```
openLooKeng can be configured to enable password encryption, these passwords will be encrypted.

Principle
-------------------------
The asymmetric encryption algorithm (RSA) is used for encrypting password.

![principle](../images/password-encryption-principal.PNG)

* Public Key: for encryption, client can use public key to encrypt plaintext.
* Private Key: for decryption, server store the private key as a keystore file in [filesystem](../develop/filesystem.md), and server can use private key to decrypt the ciphertext.

The user saves the public key and gives the private key to openLooKeng to decrypt the encrypted ciphertext.

The key suggested size of RSA is 3072 bits, the minimum is 2048 bits.

Configuration
-------------------------

To enable password encryption, you need add these properties in the `etc/config.properties`
```
security.password.decryption-type=RSA
security.key.manager-type=keystore
security.key.keystore-password=my-keystore-pwd
security.key.store-file-path=/openlookeng/keystore/keystore.jks
```

| Property                          | Description                                                  |
| :-------------------------------- | :----------------------------------------------------------- |
| `security.password.decryption-type` | The type of password decryption. Should be set to `NONE` or `RSA`. |
| `security.key.manager-type=keystore`       | The type of password encryption key storage. Should be set to `keystore`. |
| `security.key.keystore-password`          | The password of keystore.                                         |
| `security.key.cipher-transformations`          | Cipher.getInstance(transformations), the default value is 'RSA/ECB/OAEPWITHSHA256AndMGF1Padding'             |
| `security.key.store-file-path`          | The [filesystem](../develop/filesystem.md) path of keystore file.                                          |

Use Case
-------------------------
### Case 1. Create RSA key pair  
You can use keytool to create a keystore, and get Public Key from the keystore, and use openssl to encrypt data with Public Key.
And send the private key to openLooKeng server by restful api.

```
1. create a keystore, you have to use pkcs12:
keytool -genkeypair -alias alias -dname cn=openlookeng -validity 365 -keyalg RSA -keysize 2048 -keypass openlookeng -storetype jks -keystore keystore.jks -storepass openlookeng -deststoretype pkcs12

2. get Public Key from keystore, copy the public key into pub.key file:
keytool -list -rfc -keystore keystore.jks -storepass openlookeng | openssl x509 -inform pem -pubkey

3. use openssl to encrypt data with RSA/ECB/OAEPWITHSHA256AndMGF1Padding by Public Key:
openssl pkeyutl -encrypt -in data.txt -out result.en -pubin -inkey pub.key -pkeyopt rsa_padding_mode:oaep -pkeyopt rsa_oaep_md:SHA256 -pkeyopt rsa_mgf1_md:SHA256

4. get readable encrypted data by base64:
cat result.en | base64
after transfer the encrypted data by base64, you have to delete the '\n' from each line, and then you can get the final encrypted content.

5. private key 
keytool -v -importkeystore -srckeystore keystore.jks -srcstoretype jks -srcstorepass openlookeng -destkeystore server.pfx -deststoretype pkcs12 -deststorepass openlookeng -destkeypass openlookeng  
openssl pkcs12 -in server.pfx -nocerts -nodes -out private.key

the content of private.key is private key. 

6. import static catalog key pairs into keystore.jks (the keystore.jks is the value of security.key.store-file-path ) 
Assume the name of the static catalog is mysql001, we get public key from keystoer001.jks (the alias must be the same as the name of static catalog) and encrypt the data. so we should import keystore001.jks into keystore.jks. 
you can use these command bellow:
keytool -v -importkeystore -srckeystore keystore001.jks -srcstoretype jks -srcstorepass openlookeng -destkeystore server.p12 -deststoretype pkcs12 -deststorepass openlookeng -destkeypass openlookeng
keytool -importkeystore -deststorepass openlookeng -destkeystore keystore.jks -srckeystore server.p12 -srcstoretype pkcs12 -srcstorepass openlookeng -alias mysql001

``` 


### Case 2. Dynamic Catalog
A http request has the following shape (MySQL connector as an example):
```
request: POST/PUT
header: 'X-Presto-User: admin'
form: '
    catalogInformation={
        "catalogName":"mysql",
        "connectorName":"mysql",
        "securityKey":"MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC1Z4yap2cI1u6zg/R8vTcltOy8xxeOt/VG0xEArud+c5rI9h2kWy8Uo7hTFN/JapVDENT17fEzd+SqrlvcmD8ceDH07+OW2RRGcQjR0GKpKGSmubEHdH01xzpuQ1+m83B84Ir5eqcWx6QIwBPQsqqjeNpHhYdJLMpSrX1V+c7UUQIDAQAB",
        "properties":{
            "connection-url":"jdbc:mysql://localhost:3306",
            "connection-user":"root",
            "connection-password":"iRSxl1KNY06d34JGLooey0re4akzr+iJlTz1eCK1hEq8aYaX1SlzANCF7KTq6o2cF71OjINGvNjR0DXRed6gu3QYODw1Src0wiY0OvO9xfcffVt2rFvM/o238MJz1yhIcPn1BrrEgW5qVjzbbvzkS/fX+pTDqKNGAd3qefDLCuc=",
            "encrypted-properties":"connection-password",
        }
    }
'
```
* `securityKey`: The private key.
* `connection-password`: The ciphertext encrypted with private key.
* `encrypted-properties`: The encrypted property names.

Check [dynamic catalog](../admin/dynamic-catalog.md) for more information.