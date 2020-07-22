
内置系统访问控制
==============================

系统访问控制插件在任何连接器级别授权之前，对全局级别强制授权。您可以选择使用openLooKeng的任一内置的插件，或者按照[系统访问控制](../develop/system-access-control.md)中的指导提供您自己的插件。openLooKeng提供了三个内置插件：

|插件                 |说明                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| `allow-all`（缺省值） |允许所有操作。                                |
| `read-only`                 | 允许读数据或读元数据的操作，但不允许写数据或写元数据的操作。有关详细信息，请参阅”只读系统访问控制”。|
| `file`                     | 使用配置属性`security.config-file`指定的配置文件执行授权检查。有关详细信息，请参阅“基于文件的系统访问控制”。|

允许所有系统访问控制
-------------------------------

此插件允许所有操作。此插件默认开启。

只读系统访问控制
-------------------------------

通过此插件，您可以执行任何读取数据或元数据的操作，例如`SELECT`或`SHOW`。还允许设置系统级或目录级会话属性。但是禁止任何写数据或元数据的操作，如`CREATE`、`INSERT`、`DELETE`等。
要使用此插件，请添加`etc/access-control.properties`文件，文件内容如下：

``` properties
access-control.name=read-only
```

基于文件的系统访问控制
--------------------------------

此插件允许您在文件中设置访问控制规则。要使用该插件，需要添加一个`etc/access-control.properties`文件，其中必需包含两个属性：`Access-control.name`属性，必须等于`file`；`security.config-file`属性，必须等于配置文件所在的位置。例如，配置文件`rules.json`位于`etc`目录，则添加`etc/access-control.properties`文件，内容如下：

``` properties
access-control.name=file
security.config-file=etc/rules.json
```

配置文件必须是JSON格式，它包含：

-   定义哪个用户可以访问哪些目录的规则（见下面的目录规则）。
-   明确哪些主体可以标识为哪些用户的规则（见下文的主体规则）。

此插件目前仅支持目录访问控制规则和主体规则。如果您想以任何其他方式进行系统级访问限制，您必须实现一个自定义的SystemAccessControl插件（参见[系统访问控制](../develop/system-access-control.md)）。


### 刷新

默认情况下，修改`security.config-file`后，必须重新启动openLooKeng以加载所做的更改。有一个可选的属性可以不需要重启openLooKeng就能刷新属性。刷新周期在`etc/access-control.properties`中指定：

``` properties
security.refresh-period=1s
```

### 目录规则

目录规则控制特定用户可以访问的目录。根据从上到下读取匹配到的第一个规则，授予用户访问目录的权限。如果没有匹配到任何规则，则拒绝访问。每条规则由以下字段组成：


-   `user`（可选）：用于匹配用户名的正则表达式。默认为`.*`。
-   `catalog`（可选）:用于匹配目录名的正则表达式。默认为`.*`。
- `allow`（必选）: 布尔类型参数，表示用户是否有访问目录的权限


**注意**

*默认情况下，所有用户都可以访问`system`目录。您可以通过添加规则来改变此行为。*


例如，如果希望仅允许`admin`用户访问`mysql`和`system`目录，允许所有用户访问`hive`目录，拒绝其他用户访问，则可以定义以下规则：

``` json
{
  "catalogs": [
    {
      "user": "admin",
      "catalog": "(mysql|system)",
      "allow": true
    },
    {
      "catalog": "hive",
      "allow": true
    },
    {
      "catalog": "system",
      "allow": false
    }
  ]
}
```

### 主体原则

这些规则用于强制主体和指定的用户名之间的特定匹配。根据从上到下读取匹配的第一个规则，以用户身份为主体授权。
如果没有指定规则，则不进行检查。如果没有匹配到任何规则，则拒绝用户授权。每条规则由以下字段组成：

-   `principal` （必选）：要匹配的正则表达式和针对主体的组。
-   `user`（可选）：用于匹配用户名的正则表达式。如果匹配成功，则根据`allow`的值进行授权或拒绝授权。
-   `principal_to_user`（可选）：替换主体的字符串。如果替换的结果与用户名相同，则根据`allow`的值授予或拒绝授权。
    
-   `allow`（必须）:布尔类型，表示是否允许授予主体用户权限。


**注意**

*一条主体规则中至少要指定一个条件。如果在主体规则中指定了两个条件，当满足其中一个条件时，主体规则将返回期望的结论。*

以下实现LDAP认证和Kerberos认证的主体完整名称的精确匹配：

``` json
{
  "catalogs": [
    {
      "allow": true
    }
  ],
  "principals": [
    {
      "principal": "(.*)",
      "principal_to_user": "$1",
      "allow": true
    },
    {
      "principal": "([^/]+)(/.*)?@.*",
      "principal_to_user": "$1",
      "allow": true
    }
  ]
}
```

如果希望允许用户使用与其Kerberos主体名称相同的名称，并且允许`alice`和`bob`使用名为`group@example.net`的组主体，那么可以使用以下规则：

``` json
{
  "catalogs": [
    {
      "allow": true
    }
  ],
  "principals": [
    {
      "principal": "([^/]+)/?.*@example.net",
      "principal_to_user": "$1",
      "allow": true
    },
    {
      "principal": "group@example.net",
      "user": "alice|bob",
      "allow": true
    }
  ]
}
```
