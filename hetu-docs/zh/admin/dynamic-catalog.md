
# 动态目录

本节介绍openLooKeng的动态目录特性。通常openLooKeng管理员通过将目录概要文件（例如`hive.properties`）放置在连接节点目录（`etc/catalog`）下来将数据源添加到引擎。每当需要添加、更新或删除目录时，都需要重启所有协调节点和工作节点。

为了动态修改目录，openLooKeng引入了动态目录的特性。动态目录的原理是，将目录相关的配置文件在一个共享文件系统上管理，然后所有协调节点和工作节点从共享文件系统上同步到本地，并加载。开启此特性需要：

* 首先，在`etc/config.properties`中配置：

    catalog.dynamic-enabled=true

* 其次，在`hdfs-config-default.properties`中配置用于存储动态目录信息的文件系统。你可以通过`etc/node.properties`中的`catalog.share.filesystem.profile`属性修改这个文件名，默认为`hdfs-config-default`，你可以查看[文件系统文档](../develop/filesystem.md )以获取更多信息。

  在`etc/filesystem/`路径下添加`hdfs-config-default.properties`文件， 如果这个路径不存在，请创建。

  ```
  fs.client.type=hdfs
  hdfs.config.resources=/opt/openlookeng/config/core-site.xml, /opt/openlookeng/config/hdfs-site.xml
  hdfs.authentication.type=NONE
  fs.hdfs.impl.disable.cache=true
  ```

  如果HDFS开启Kerberos认证，那么

  ```
  fs.client.type=hdfs
  hdfs.config.resources=/opt/openlookeng/config/core-site.xml, /opt/openlookeng/config/hdfs-site.xml
  hdfs.authentication.type=KERBEROS
  hdfs.krb5.conf.path=/opt/openlookeng/config/krb5.conf
  hdfs.krb5.keytab.path=/opt/openlookeng/config/user.keytab
  hdfs.krb5.principal=openlookeng@HADOOP.COM # replace openlookeng@HADOOP.COM to your principal 
  fs.hdfs.impl.disable.cache=true
  ```

* 最后，在`etc/node.properties`配置用户文件系统中的存储动态目录信息的路径，用于指定共享文件系统上与本地存放目录相关的配置文件的路径；同时因为需要从共享文件系统上的相同路径同步配置文件，所以所有协调节点和工作节点的共享文件系统上的路径必须一致，本地的存放路径不做要求。

  ```
  catalog.config-dir=/opt/openlookeng/catalog
  catalog.share.config-dir=/opt/openkeng/catalog/share
  ```

  

## 使用

目录操作是通过openLooKeng协调节点上的RESTful API来完成的。HTTP请求具有如下形态（以Hive连接节点为例），POST/PUT请求体形式为`multipart/form-data`：

```shell
curl --location --request POST 'http://your_coordinator_ip:9101/v1/catalog' \
--header 'X-Presto-User: admin' \
--form 'catalogInformation="{
        \"catalogName\" : \"hive\",
        \"connectorName\" : \"hive-hadoop2\",
        \"properties\" : {
              \"hive.hdfs.impersonation.enabled\" : \"false\",
              \"hive.hdfs.authentication.type\" : \"KERBEROS\",
              \"hive.collect-column-statistics-on-write\" : \"true\",
              \"hive.metastore.service.principal\" : \"hive/hadoop.hadoop.com@HADOOP.COM\",
              \"hive.metastore.authentication.type\" : \"KERBEROS\",
              \"hive.metastore.uri\" : \"thrift://xx.xx.xx.xx:21088\",
              \"hive.allow-drop-table\" : \"true\",
              \"hive.config.resources\" : \"core-site.xml,hdfs-site.xml\",
              \"hive.hdfs.presto.keytab\" : \"user.keytab\",
              \"hive.metastore.krb5.conf.path\" : \"krb5.conf\",
              \"hive.metastore.client.keytab\" : \"user.keytab\",
              \"hive.metastore.client.principal\" : \"test@HADOOP.COM\",
              \"hive.hdfs.wire-encryption.enabled\" : \"true\",
              \"hive.hdfs.presto.principal\" : \"test@HADOOP.COM\"
              }
          }
"' \
--form 'catalogConfigurationFiles=@"/path/to/core-site.xml"' \
--form 'catalogConfigurationFiles=@"/path/to/hdfs-site.xml"' \
--form 'catalogConfigurationFiles=@"/path/to/user.keytab"', \
--form 'globalConfigurationFiles=@"/path/to/krb5.conf"'
```

`catalogName`为用户指定的目录名称；

`connectorName`为目录类型，请参考connector章节；

`properties`为指定目录类型的相关参数，请参考各个connector的详细配置；

配置中如果需要指定文件，在`properties`中只需要指定文件名称即可，文件本地的路径以以下方式进行设置：

1. 如果配置文件是所有目录共同使用，那么将文件路径放入`globalConfigurationFiles`参数中，例如`krb5.conf`；
2. 如果配置文件只有当前创建的目录使用，那么将文件路径放入`catalogConfigurationFiles`参数中，例如`hdfs-site.xml`、`core-site.xml`、`user.keytab`，每个目录均有不同的配置。


### 添加目录

当添加新目录时，会向协调节点发送一个POST请求。协调节点首先重写文件路径属性，将文件保存到本地磁盘，并通过加载新添加的目录来验证操作。如果目录加载成功，协调节点将文件保存到共享文件系统（例如HDFS）。

其他协调节点和工作节点定期检查共享文件系统中的目录属性文件。当发现新的目录时，他们把相关的配置文件拉到本地磁盘，然后将目录加载到内存中。

以Hive为例，通过curl可以用以下命令创建目录：

```shell
curl --location --request POST 'http://your_coordinator_ip:8090/v1/catalog' \
--header 'X-Presto-User: admin' \
--form 'catalogInformation="{
        \"catalogName\" : \"hive\",
        \"connectorName\" : \"hive-hadoop2\",
        \"properties\" : {
              \"hive.hdfs.impersonation.enabled\" : \"false\",
              \"hive.hdfs.authentication.type\" : \"KERBEROS\",
              \"hive.collect-column-statistics-on-write\" : \"true\",
              \"hive.metastore.service.principal\" : \"hive/hadoop.hadoop.com@HADOOP.COM\",
              \"hive.metastore.authentication.type\" : \"KERBEROS\",
              \"hive.metastore.uri\" : \"thrift://xx.xx.xx.xx:21088\",
              \"hive.allow-drop-table\" : \"true\",
              \"hive.config.resources\" : \"core-site.xml,hdfs-site.xml\",
              \"hive.hdfs.presto.keytab\" : \"user.keytab\",
              \"hive.metastore.krb5.conf.path\" : \"krb5.conf\",
              \"hive.metastore.client.keytab\" : \"user.keytab\",
              \"hive.metastore.client.principal\" : \"test@HADOOP.COM\",
              \"hive.hdfs.wire-encryption.enabled\" : \"true\",
              \"hive.hdfs.presto.principal\" : \"test@HADOOP.COM\"
              }
          }
"' \
--form 'catalogConfigurationFiles=@"/path/to/core-site.xml"' \
--form 'catalogConfigurationFiles=@"/path/to/hdfs-site.xml"' \
--form 'catalogConfigurationFiles=@"/path/to/user.keytab"', \
--form 'globalConfigurationFiles=@"/path/to/krb5.conf"'
```

### 删除目录

与添加操作类似，当需要删除目录时，向协调节点发送DELETE请求。接收请求的协调节点从本地磁盘中删除相关目录概要文件，从服务器卸载目录，并从共享文件系统中删除目录。

其他协调节点和工作节点定期检查共享文件系统中的目录属性文件。当删除目录时，协调节点和工作节点也会删除本地磁盘上的相关配置文件，然后从内存中卸载目录。

以Hive为例，通过curl可以用以下命令删除目录，在`catalog`后指定之前创建过的目录名称：

```shell
curl --location --request DELETE 'http://your_coordinator_ip:8090/v1/catalog/hive' \
--header 'X-Presto-User: admin'
```

### 更新目录

UPDATE操作是DELETE和ADD操作的组合。首先管理员向协调节点发送PUT请求。协调节点收到请求后在本地删除并添加目录以验证更改。如果操作成功，协调节点从共享文件系统中删除目录，并等待所有其他节点从本地文件系统中删除目录。将新的配置文件保存到共享文件系统中。

其他协调节点和工作节点定期检查共享文件系统中的目录属性文件，并在本地文件系统上执行相应的更改。

目录属性，包括 `connector-name`、`properties`等，支持修改。但是**目录名称**不能更改。

以Hive为例，通过curl可以用以下命令更新目录，以下更新了`hive.allow-drop-table`参数：

```shell
curl --location --request PUT 'http://your_coordinator_ip:8090/v1/catalog' \
--header 'X-Presto-User: admin' \
--form 'catalogInformation="{
        \"catalogName\" : \"hive\",
        \"connectorName\" : \"hive-hadoop2\",
        \"properties\" : {
              \"hive.hdfs.impersonation.enabled\" : \"false\",
              \"hive.hdfs.authentication.type\" : \"KERBEROS\",
              \"hive.collect-column-statistics-on-write\" : \"true\",
              \"hive.metastore.service.principal\" : \"hive/hadoop.hadoop.com@HADOOP.COM\",
              \"hive.metastore.authentication.type\" : \"KERBEROS\",
              \"hive.metastore.uri\" : \"thrift://xx.xx.xx.xx:21088\",
              \"hive.allow-drop-table\" : \"false\",
              \"hive.config.resources\" : \"core-site.xml,hdfs-site.xml\",
              \"hive.hdfs.presto.keytab\" : \"user.keytab\",
              \"hive.metastore.krb5.conf.path\" : \"krb5.conf\",
              \"hive.metastore.client.keytab\" : \"user.keytab\",
              \"hive.metastore.client.principal\" : \"test@HADOOP.COM\",
              \"hive.hdfs.wire-encryption.enabled\" : \"true\",
              \"hive.hdfs.presto.principal\" : \"test@HADOOP.COM\"
              }
          }
"' \
--form 'catalogConfigurationFiles=@"/path/to/core-site.xml"' \
--form 'catalogConfigurationFiles=@"/path/to/hdfs-site.xml"' \
--form 'catalogConfigurationFiles=@"/path/to/user.keytab"', \
--form 'globalConfigurationFiles=@"/path/to/krb5.conf"'
```



## API信息

### HTTP请求

添加：`POST host/v1/catalog`

更新：`PUT host/v1/catalog`

删除：`DELETE host/v1/catalog/{catalogName}`

### HTTP返回码

| HTTP返回码| POST| PUT| DELETE|
|----------|----------|----------|----------|
| 401 UNAUTHORIZED| 没有权限添加目录| 没有权限修改目录| 同PUT|
| 302 FOUND| 目录已存在| \-| \-|
| 404 NOT\_FOUND| 动态目录已停用| 目录不存在或动态目录已停用| 同PUT|
| 400 BAD\_REQUEST| 请求不正确| 同POST| 同PUT|
| 409 CONFLICT| 另一个会话正在操作目录| 同POST| 同POST|
| 500 INTERNAL\_SERVER\_ERROR| 协调节点内部发生错误| 同POST| 同POST|
| 201 CREATED| 成功| 成功| \-|
| 204 NO\_CONTENT| \-| \-| 成功|

## 配置属性

在`etc/config.properties`中：

| 属性名称| 是否必选| 描述| 默认值|
|----------|----------|----------|----------|
| `catalog.dynamic-enabled`| 否| 是否启用动态目录。 | false|
| `catalog.scanner-interval`| 否| 扫描共享文件系统中目录的时间间隔。| 5s|
| `catalog.max-file-size`| 否| 目录文件最大大小，用于限制上传的最大文件大小，避免客户端上传的文件过大。 | 128 KB|
| `catalog.valid-file-suffixes`| 否| 有效的配置文件后缀名，如果有多个，以逗号分开；当不配置时，允许所有的后缀类型。用于校验上传的文件类型，指定类型的配置文件才允许上传。 | |

在`etc/node.properties`中：

路径配置白名单：["/tmp", "/opt/hetu", "/opt/openlookeng", "/etc/hetu", "/etc/openlookeng", 工作目录]

注意：避免选择根目录；路径不能包含../；如果配置了node.data_dir,那么当前工作目录为node.data_dir的父目录；
    如果没有配置，那么当前工作目录为openlookeng server的目录

| 属性名称| 是否必选| 描述| 默认值|
|----------|----------|----------|----------|
| `catalog.config-dir`| 是| 本地磁盘存放配置文件的根目录。||
| `catalog.share.config-dir`| 是 | 共享文件系统中存放配置文件的根目录。||
| `catalog.share.filesystem.profile` | 否 | 共享文件系统的配置文件名。 |hdfs-config-default|

## 对查询的影响

- 删除目录后，正在执行的查询可能会失败。
- 更新目录时，正在进行的查询可能会失败。