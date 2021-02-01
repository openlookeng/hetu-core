
认证用户映射
===================

认证用户映射定义了从认证系统中的用户到openLooKeng用户的映射规则。认证用户映射对于具有例如`alice@example`或`CN=Alice Smith, OU=Finance, O=Acme, C=US`等复杂用户名的Kerberos或LDAP认证特别重要。

可以使用简单的正则表达式模式匹配配置用户映射，也可以在单独的配置文件中配置更复杂的映射规则。

### 模式映射规则

模式映射规则将认证用户映射到正则表达式中的第一个匹配组。如果正则表达式与认证用户不匹配，则拒绝认证。

每个身份认证系统都具有单独的用户映射模式的属性，以在启用多个身份认证系统时允许不同的映射：

| 认证方式                      | 属性                                                           |
| ---------------------------- | ------------------------------------------------------------- |
| Username and Password (LDAP) | `http-server.authentication.password.user-mapping.pattern`    |
| Kerberos                     | `http-server.authentication.krb5.user-mapping.pattern`        |
| Certificate                  | `http-server.authentication.certificate.user-mapping.pattern` |
| Json Web Token               | `http-server.authentication.jwt.user-mapping.pattern`         |

### 文件映射规则

文件映射规则允许认证用户配置更复杂的映射。这些映射规则是从配置属性中定义的JSON文件加载的，基于从上到下第一个匹配的规则进行映射，如果没有规则匹配，则认证被拒绝。每个规则由以下字段组成：

| 字段名称        | 默认值         | 是否必填  | 描述                       |
| -------------- | ------------- | ------- | ------------------------- |
| pattern        | (none)        | 是       | 用于认证用户匹配的正则表达式   |
| user           | `$1`          | 否       | 替换模式匹配的字符串          |
| allow          | true          | 否       | 布尔值，指示是否允许认证       |

如以下示例所示，除了`test`用户拒绝认证外，会将`alice@example.com`用户映射到`alice`，并将`bob@uk.example.com`之类的用户映射到`bob_uk`：

``` json
{
    "rules": [
        {
            "pattern": "test@example\\.com",
            "allow": false
        },
        {
            "pattern": "(.+)@example\\.com"
        },
        {
            "pattern": "(?<user>.+)@(?<region>.+)\\.example\\.com",
            "user": "${user}_${region}"
        }
    ]
}
```

每个身份认证系统都有一个用于用户映射文件的单独属性，以在启用多个身份认证系统时允许不同的映射：

| 认证方式                      | 属性                                                           |
| ---------------------------- | ------------------------------------------------------------- |
| Username and Password (LDAP) | `http-server.authentication.password.user-mapping.file`       |
| Kerberos                     | `http-server.authentication.krb5.user-mapping.file`           |
| Certificate                  | `http-server.authentication.certificate.user-mapping.file`    |
| Json Web Token               | `http-server.authentication.jwt.user-mapping.file`            |