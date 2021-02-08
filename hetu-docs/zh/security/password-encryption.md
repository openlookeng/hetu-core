
密码加密
===================

概要
-------------------------
openLooKeng 管理一些catalog相关的配置文件，这些配置文件可能包含一些用户名、密码等信息，其中，密码信息一般情况下要求保密，不能以明文的方式存储。
这是一个典型的MySQL connector的配置文件：
```
connector.name=mysql
connection-url=jdbc:mysql://localhost:3306
connection-user=root
connection-password=123456
```
openLooKeng 可以开启密码加密功能，这些密码就可以被加密存储。

建议RSA Key的长度为3072 bit，最低2048 bit。

原理
-------------------------
我们采用非对称加密算法 (RSA)，原理如下：

![password-encryption-principle](../images/password-encryption-principal.PNG)

* 私钥： 用于加密，客户端使用私钥对明文进行加密。
* 公钥： 用户解密，服务端将公钥存储在[文件系统](../develop/filesystem.md )的keystore中，并使用公钥对密文进行解密。

用户自己保存秘钥，将公钥给openLooKeng用于加密后的密文解密。

配置
-------------------------

为了开启密码加密特性，你需要在 `etc/config.properties`增加以下属性：
```
security.password.decryption-type=RSA
security.key.manager-type=keystore
security.key.keystore-password=my-keystore-pwd
security.key.store-file-path=/openlookeng/keystore/keystore.jks
```

| 属性                          | 描述                                                  |
| :-------------------------------- | :----------------------------------------------------------- |
| `security.password.decryption-type` | 密码加解密使用的加密算法. 必须是 `NONE` 或 `RSA`. |
| `security.key.manager-type=keystore`       | 加密秘钥的存储方式. 必须是 `keystore`. |
| `security.key.keystore-password`          | keystore的密码.                                         |
| `security.key.store-file-path`          | [文件系统](../develop/filesystem.md) 中keystore文件的路径.                                          |

用例
-------------------------
### 用例 1. 动态目录
一个http请求的模板如下 (以MySQL connector为例):
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
* `securityKey`: 公钥.
* `connection-password`: 使用私钥加密后的密码密文.
* `encrypted-properties`: 加密的属性名称.

可以从 [动态目录](../admin/dynamic-catalog.md) 查看更多信息.