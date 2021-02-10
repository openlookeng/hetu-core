
# Hive安全配置

## 授权

可以通过在Hive目录属性文件中设置`hive.security`属性来启用[Hive](./hive.md)的授权检查。此属性必须是下列值之一：

| 属性值| 说明|
|:----------|:----------|
| `legacy`（默认值）| 授权检查很少执行，因此大多数操作都是允许的。使用配置属性`hive.allow-drop-table`、`hive.allow-rename-table`、`hive.allow-add-column`、`hive.allow-drop-column`和`hive.allow-rename-column`。|
| `read-only`| 允许读数据或元数据的操作（如`SELECT`），不允许写数据或元数据的操作（如`CREATE`、`INSERT`或`DELETE`）。|
| `file`| 授权检查使用Hive配置属性`security.config-file`指定的配置文件来执行。有关详细信息，请参阅[基于文件的授权](./hive-security.md#基于文件的授权)。|
| `sql-standard`| 按照SQL标准，只要用户具有所需的权限，就可以执行这些操作。在这种模式下，openLooKeng基于Hive元存储中定义的权限对查询执行授权检查。若要更改这些权限，使用[GRANT](../sql/grant.md)和[REVOKE](../sql/revoke.md)命令。有关详细信息，请参阅[基于SQL标准的授权](./hive-security.md#基于sql标准的授权)。|

### 基于SQL标准的授权

当启用`sql-standard`安全时，openLooKeng与Hive一样执行基于SQL标准的授权。

由于openLooKeng的`ROLE`语法支持符合SQL标准，而Hive并不完全遵循SQL标准，因此存在以下限制和差异：

- 不支持`CREATE ROLE role WITH ADMIN`。
- 必须启用`admin`角色才能执行`CREATE ROLE`或`DROP ROLE`。
- 不支持`GRANT role TO user GRANTED BY someone`。
- 不支持`REVOKE role FROM user GRANTED BY someone`。
- 默认在新的用户会话中，用户除角色`admin`外的所有角色都启用。
- 通过执行`SET ROLE role`可以选择特定的角色。
- `SET ROLE ALL`启用用户除`admin`以外的所有角色。
- `admin`角色必须通过执行`SET ROLE admin`显式启用。

## 身份验证

`/connector/hive`默认的安全配置在连接Hadoop集群时不使用身份验证。无论哪个用户提交查询，所有查询都以运行openLooKeng进程的用户执行。

Hive连接器提供了额外的安全选项来支持配置为使用[Kerberos](#kerberos支持)的Hadoop集群。

访问`HDFS (Hadoop Distributed File System)`时，openLooKeng可以[模拟](./hive-security.md#终端用户模拟)运行查询的最终用户。这可以与HDFS权限和`ACLs (Access Control Lists)`一起使用，为数据提供进一步的安全性。



**警告**

当对Hadoop服务使用Kerberos身份验证时，应该使用Kerberos来保护对openLooKeng协调节点的访问。未能安全地访问openLooKeng协调节点可能导致Hadoop集群上的敏感数据被未经授权访问。

有关设置Kerberos身份验证的信息，请参阅[协调节点Kerberos认证](../security/server.md)和[CLI接口的Kerberos认证](../security/cli.md)。

## Kerberos支持

为了将Hive连接器与使用`kerberos`身份验证的Hadoop集群一起使用，需要将连接器配置为与Hadoop集群上的两个服务一起使用：

- Hive元存储Thrift服务
- Hadoop分布式文件系统（HDFS）

Hive连接器对这些服务的访问是在属性文件中配置的，该文件包含Hive连接器的一般配置。

**说明**

如果`krb5.conf`位置与`/etc/krb5.conf`不同，则必须在`jvm.config`文件中使用`java.security.krb5.conf` JVM属性显式设置。

示例： `-Djava.security.krb5.conf=/example/path/krb5.conf`.

### Hive元存储Thrift服务身份验证说明

在Kerberos化Hadoop集群中，openLooKeng使用`SASL (Simple Authentication and Security Layer)`连接Hive 元存储Thrift服务并使用Kerberos进行身份验证。连接器的属性文件中使用以下属性配置元存储的Kerberos身份验证：

| 属性名称| 说明|
|:----------|:----------|
| `hive.metastore.authentication.type`| Hive元存储身份验证类型。|
| `hive.metastore.thrift.impersonation.enabled`| 启用Hive元存储用户模拟。|
| `hive.metastore.service.principal`| Hive元存储服务的Kerberos主体。|
| `hive.metastore.client.principal`| openLooKeng在连接到Hive元存储服务时将使用的Kerberos主体。|
| `hive.metastore.client.keytab`| Hive元存储客户端keytab位置。|
| `hive.metastore.krb5.conf.path`| Kerberos配置文件位置。|

#### `hive.metastore.authentication.type`

`NONE`或`KERBEROS`。使用默认值`NONE`时，Kerberos身份验证被禁用，无需配置其他属性。

当设置为`KERBEROS`时，Hive连接器将使用SASL连接到Hive元存储Thrift服务，并使用Kerberos进行身份验证。

此属性是可选的；默认值为`NONE`。

#### `hive.metastore.service.principal`

Hive元存储服务的Kerberos主体。openLooKeng协调节点将使用此属性对Hive元存储进行身份验证。

此属性值中可使用`_HOST`占位符。当连接到Hive元存储时，Hive连接器将在其所连接的**元存储**服务器的主机名中替换。如果元存储在多个主机上运行，此属性非常有用。

示例：`hive/hive-server-host@EXAMPLE.COM`或`hive/_HOST@EXAMPLE.COM`。

此属性是可选的；无默认值。

#### `hive.metastore.client.principal`

openLooKeng在连接到Hive元存储时将使用的Kerberos主体。

此属性值中可使用`_HOST`占位符。当连接到Hive元存储时，Hive连接器会替换openLooKeng所在的工作节点的主机名。如果每个工作节点都有自己的Kerberos主体，此属性非常有用。

示例：`openlookeng/openlookeng-server-node@EXAMPLE.COM`或`openlookeng/_HOST@EXAMPLE.COM`。

此属性是可选的；无默认值。

**警告**

`hive.metastore.client.principal`指定的主体必须具有足够的权限来移除`hive/warehouse`目录中的文件和目录。如果主体没有权限，则只移除元数据，数据将继续占用磁盘空间。

这是由于Hive元存储负责删除内部表数据。当元存储配置为使用Kerberos身份验证时，元存储执行的所有HDFS操作都会被模拟。删除数据的错误将被忽略。

#### `hive.metastore.client.keytab`

keytab文件的路径，该文件包含由`hive.metastore.client.principal`指定的主体的密钥。该文件必须可由运行openLooKeng的操作系统用户读取。

此属性是可选的；无默认值。

#### `hive.metastore.krb5.conf.path`

Kerberos配置文件的路径。该文件必须可由运行openLooKeng的操作系统用户读取。

此属性是可选的；无默认值。

#### `NONE`身份验证配置示例

``` properties
hive.metastore.authentication.type=NONE
```

Hive元存储默认的身份验证类型为`NONE`。当身份验证类型为`NONE`时，openLooKeng连接不安全的Hive元存储。不使用Kerberos。

#### `KERBEROS`身份验证配置示例

``` properties
hive.metastore.authentication.type=KERBEROS
hive.metastore.thrift.impersonation.enabled=true
hive.metastore.service.principal=hive/hive-metastore-host.example.com@EXAMPLE.COM
hive.metastore.client.principal=openlk@EXAMPLE.COM
hive.metastore.client.keytab=/etc/openlookeng/hive.keytab
hive.metastore.krb5.conf.path=/etc/openlookeng/krb5.conf
```

当Hive元存储Thrift服务的身份验证类型为`KERBEROS`时，openLooKeng将作为属性`hive.metastore.client.principal`指定的Kerberos主体进行连接。openLooKeng将使用`hive.metastore.client.keytab`属性指定的keytab对主体进行身份验证，并将验证元存储的标识是否与`hive.metastore.service.principal`匹配。

Keytab文件需要分发到集群中每一个运行openLooKeng的节点。

[Keytab文件附加信息](./hive-security.md#keytab文件附加信息)

### HDFS身份验证

在Kerberos化的Hadoop集群中，openLooKeng使用Kerberos对HDFS进行身份验证。连接器的属性文件中使用以下属性配置HDFS的Kerberos身份验证：

| 属性名称| 说明|
|:----------|:----------|
| `hive.hdfs.authentication.type`| HDFS身份验证类型。取值为`NONE`或`KERBEROS`。|
| `hive.hdfs.impersonation.enabled`| 启用HDFS终端用户模拟。|
| `hive.hdfs.presto.principal`| openLooKeng在连接到HDFS时将使用的Kerberos主体。|
| `hive.hdfs.presto.keytab`| HDFS客户端keytab位置。|
| `hive.hdfs.wire-encryption.enabled`| 启用HDFS有线加密。|

#### `hive.hdfs.authentication.type`

`NONE`或`KERBEROS`。使用默认值`NONE`时，Kerberos身份验证被禁用，无需配置其他属性。

当设置为`KERBEROS`时，Hive连接器将使用Kerberos对HDFS进行身份验证。

此属性是可选的；默认值为`NONE`。

#### `hive.hdfs.impersonation.enabled`

启用终端用户HDFS模拟。

`End User Impersonation`章节将深入介绍HDFS模拟。

此属性是可选的；默认值为`false`。

#### `hive.hdfs.presto.principal`

openLooKeng在连接到HDFS时将使用的Kerberos主体。

此属性值中可使用`_HOST`占位符。当连接到HDFS时，Hive连接器会替换openLooKeng所在的工作节点的主机名。如果每个工作节点都有自己的Kerberos主体，此属性非常有用。

示例：`openlookeng-hdfs-superuser/openlookeng-server-node@EXAMPLE.COM`

或`openlookeng-hdfs-superuser/_HOST@EXAMPLE.COM`。

此属性是可选的；无默认值。

#### `hive.hdfs.presto.keytab`

keytab文件的路径，该文件包含由`hive.hdfs.presto.principal`指定的主体的密钥。该文件必须可由运行openLooKeng的操作系统用户读取。

此属性是可选的；无默认值。

#### `hive.hdfs.wire-encryption.enabled`

在使用使用HDFS有线加密的Kerberos化Hadoop集群中，此属性需要设置为`true`，允许openLooKeng访问HDFS。注意，使用有线加密可能会影响查询执行性能。

#### `NONE`身份验证配置示例

``` properties
hive.hdfs.authentication.type=NONE
```

HDFS默认的身份验证类型为`NONE`。当身份验证类型为`NONE`时，openLooKeng通过Hadoop的简单身份验证机制连接HDFS。不使用Kerberos。

#### `KERBEROS`身份验证配置示例

``` properties}
hive.hdfs.authentication.type=KERBEROS
hive.hdfs.presto.principal=hdfs@EXAMPLE.COM
hive.hdfs.presto.keytab=/etc/openlookeng/hdfs.keytab
```

当身份验证类型为`KERBEROS`时，openLooKeng作为`hive.hdfs.presto.principal`属性指定的主体访问HDFS。openLooKeng将使用`hive.hdfs.presto.keytab` keytab指定的keytab来对该主体进行身份验证。

Keytab文件需要分发到集群中每一个运行openLooKeng的节点。

[Keytab文件附加信息](./hive-security.md#keytab文件附加信息)

## 终端用户模拟

### 模拟访问HDFS

openLooKeng可以模拟运行查询的终端用户。对于从命令行界面运行查询的用户，最终用户是与openLooKeng CLI进程或可选`--user`选项的参数关联的用户名。如果使用了HDFS权限或ACL，模拟最终用户可以在访问HDFS时提供进一步的安全性。

HDFS权限和ACL在[HDFS权限指南](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html )中有说明。

#### 使用HDFS模拟进行`NONE`身份验证

``` properties
hive.hdfs.authentication.type=NONE
hive.hdfs.impersonation.enabled=true
```

当使用模拟进行`NONE`身份验证时，openLooKeng在访问HDFS时模拟运行查询的用户。如`configuring-hadoop-impersonation`章节中所述，运行openLooKeng的用户必须允许模拟此用户。不使用Kerberos。

#### 使用HDFS模拟进行`KERBEROS`身份验证

``` properties
hive.hdfs.authentication.type=KERBEROS
hive.hdfs.impersonation.enabled=true
hive.hdfs.presto.principal=openlk@EXAMPLE.COM
hive.hdfs.presto.keytab=/etc/openlookeng/hdfs.keytab
```

当使用模拟进行`KERBEROS`身份验证时，openLooKeng在访问HDFS时模拟运行查询的用户。如`configuring-hadoop-impersonation`章节中所述，`hive.hdfs.presto.principal`指定的主体必须允许模拟此用户。openLooKeng使用`hive.hdfs.presto.keytab`指定的keytab对`hive.hdfs.presto.principal`进行身份验证。

Keytab文件需要分发到集群中每一个运行openLooKeng的节点。

[Keytab文件附加信息](#keytab文件附加信息)

### 模拟访问Hive元存储

openLookeng支持在访问Hive元存储时模拟最终用户,可以使用以下配置启用元存储模拟

    hive.metastore.thrift.impersonation.enabled=true

当用户模拟时使用`KERBEROS`元存储身份验证时，通过配置`hive.metastore.client.principal`属性指定的主体必须允许当前的openLooKeng用户来模拟，如[在Hadoop中模拟]（# 在Hadoop中模拟）章节所述。

### 在Hadoop中模拟

为了**使用HDFS模拟进行`NONE`身份验证**或使用**HDFS模拟进行`KERBEROS`身份验证**，Hadoop集群必须配置为允许openLooKeng正在运行的用户或主体模拟登录到openLooKeng的用户。Hadoop模拟配置在`core-site.xml`文件中。配置选项的完整描述见[Hadoop文档](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html#Configurations )。

## Keytab文件附加信息

Keytab文件包含加密密钥，用于对Kerberos `KDC (Key Distribution Center)`的主体进行身份验证。必须安全地存储这些加密密钥；应采取与保护SSH私钥相同的预防措施来保护。

特别是，对keytab文件的访问应该仅限于实际需要使用它们进行身份验证的账号。在实际操作中，这个账号是运行openLooKeng进程的用户。应设置keytab文件的属主和权限，防止其他用户读取或修改keytab文件。

需要将keytab文件分发到每个运行openLooKeng的节点。在普通部署情况下，Hive连接器在所有节点上的配置都相同。这意味着keytab需要在每个节点上的同一位置。

分发keytab文件后，需要确保keytab文件在每个节点上都有正确的权限。

## 基于文件的授权

config文件使用JSON指定，由三个部分组成，每个部分都是按config文件中指定的顺序匹配的规则列表。授予用户第一个匹配规则的权限。如果不指定，所有正则表达式都默认为`.*`。

### 模式规则

这些规则控制着谁被认为是一个模式的所有者。

- `user`（可选）：用于匹配用户名的正则表达式。
- `schema`（可选）：用于匹配模式名的正则表达式。
- `owner`（必选）：boolean类型，表示所有权。

### 表规则

这些规则管理授予特定表的权限。

- `user`（可选）：用于匹配用户名的正则表达式。
- `schema`（可选）：用于匹配模式名的正则表达式。
- `table`（可选）：用于匹配表名的正则表达式。
- `privileges`（必选）：`SELECT`、`INSERT`、`DELETE`、`OWNERSHIP`、`GRANT_SELECT`中的零个或多个。

### 会话属性规则

这些规则控制谁可以设置会话属性。

- `user`（可选）：用于匹配用户名的正则表达式。
- `property`（可选）：用于匹配会话属性名的正则表达式。
- `allowed`（必选）：boolean类型，是否设置该会话属性。

示例如下：

``` json
{
  "schemas": [
    {
      "user": "admin",
      "schema": ".*",
      "owner": true
    },
    {
      "user": "guest",
      "owner": false
    },
    {
      "schema": "default",
      "owner": true
    }
  ],
  "tables": [
    {
      "user": "admin",
      "privileges": ["SELECT", "INSERT", "DELETE", "OWNERSHIP"]
    },
    {
      "user": "banned_user",
      "privileges": []
    },
    {
      "schema": "default",
      "table": ".*",
      "privileges": ["SELECT"]
    }
  ],
  "sessionProperties": [
    {
      "property": "force_local_scheduling",
      "allow": true
    },
    {
      "user": "admin",
      "property": "max_split_size",
      "allow": true
    }
  ]
}
```