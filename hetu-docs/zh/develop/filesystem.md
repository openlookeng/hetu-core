
# 文件系统访问实用程序

## 概述

openLooKeng 项目包含一组文件系统客户端实用程序，用于帮助访问和修改文件。当前支持两组文件系统：HDFS 和本地文件系统。SPI 中提供了 ``HetuFileSystemClient`` 接口，该接口定义了要在项目中使用的常用文件操作。该客户端的目标是在不同的文件系统上提供统一的接口、行为和异常。因此，客户端代码可以轻松地重用代码，无需更改代码即可传递其逻辑。

SPI 中的实用程序类 `FileBasedLock` 可以提供对给定的文件系统的独占访问权限。该类利用统一的文件系统客户端接口，因此可用于不同的文件系统。

文件系统客户端实现为 openLooKeng 插件。实现放置在 ``hetu-filesystem-client`` 模块中，在该模块中，必须在 ``HetuFileSystemClientPlugin`` 中实现和注册一个扩展 ``HetuFileSystemClientFactory`` 的工厂。

## 文件系统配置文件

文件系统配置文件必须包含一个类型字段：

    fs.client.type=<filesystem type>

可以提供相应的文件系统客户端使用的附加配置。例如，HDFS 文件系统需要配置资源文件 `core-site.xml` 和 `hdfs-site.xml` 的路径。如果文件系统启用了身份验证（例如 Kerberos），那么还应在配置文件中指定凭据。

可以将多个定义文件系统客户端的配置文件放置在 `etc/filesystem` 中。  与目录类似，这些配置文件将在服务器启动时由文件系统管理器加载，该管理器还生成多个文件系统客户端，以在 `presto-main` 中使用。客户端模块可以自行读取该文件夹中的配置文件，但强烈建议客户端模块获取由主模块提供的客户端，以防止出现依赖问题。

拥有多个配置文件的典型用例是使用并预设多个 HDFS。在这种情况下，为每个集群创建一个属性文件并在不同的配置文件中包含其各自的身份验证信息，例如 `hdfs1.properties` 和 `hdfs2.properties`。客户端代码将能够通过指定配置文件名称（文件名）来访问这些属性文件，如 `FileSystemClientManager` 中的 `getFileSystemClient("hdfs1", <rootPath>)`。

每一个本地和HDFS的文件系统客户端在构造时都要求一个访问限制的根目录，在访问文件系统时客户端会检查访问的文件或目录是否在这个根目录内，不在此目录内的访问会被禁止。建议在配置时使用满足使用需求的最小的一个目录，以避免不慎修改（或删除）本来不该访问的文件。

### 文件系统配置文件属性

| 属性名称| 是否必选| 说明| 默认值|
|----------|----------|----------|----------|
| `fs.client.type`| 是| 文件系统配置文件的类型。接受的值：`local`、`hdfs`。||
| `hdfs.config.resources`| 否| HDFS 资源文件（例如 core-site.xml 和 hdfs-site.xml）的路径。| 使用本地 HDFS|
| `hdfs.authentication.type`| 是| HDFS 身份验证。接受的值：`KERBEROS`、`NONE`。||
| `hdfs.krb5.conf.path`| 是（如果身份验证类型设置为 Kerberos）| Krb5 配置文件的路径||
| `hdfs.krb5.keytab.path`| 是（如果身份验证类型设置为 Kerberos）| Kerberos keytab 文件的路径||
| `hdfs.krb5.principal`| 是（如果身份验证类型设置为 Kerberos）| Kerberos 身份验证的主体 ||
| `fs.hdfs.impl.disable.cache`| 否| 在 HDFS 中禁用高速缓存。| `false`|

openLooKengFileSystemClient

``HetuFileSystemClient`` 统一为以下各项设置文件系统访问标准：

- 方法签名
- 行为
- 异常

实现所有这些统一的最终目标是提高在多个文件系统上执行文件操作所需的代码的可重用性。

### 方法签名和行为

方法签名与 `java.nio.file.spi.FileSystemProvider` 和 `java.nio.file.Files` 中的签名相同，以最大程度地提高兼容性。此外，还额外提供了 `java.nio` 包中不存在的一些方法，以提供有用的功能（例如 `deleteRecursively()`）。

### 异常

与方法签名类似，异常模式严格遵循与 `Files` 中生成的异常相同的模式。对于其他文件系统，实现将异常转换为在语义上基本等价的 Java 本地 `java.nio.file.FileSystemException`。这使客户端代码能够：

1. 处理 `IOException`，而无需担心基础文件系统。
2. 与额外的依赖项（如 Hadoop）分离。

以下是从 HDFS 进行转换的异常示例：

- `org.apache.hadoop.fs.FileAlreadyExistsException` -> `java.nio.file.FileAlreadyExistsException`
- `org.apache.hadoop.fs.PathIsNotEmptyDirectoryException` -> `java.nio.file.DirectoryNotEmptyException`

## FileBasedLock

在 `io.prestosql.spi.filesystem` 中提供了 `FileBasedLock` 实用程序，该实用程序需要与 ``HetuFileSystemClient`` 一起使用。该锁是基于文件的，并且遵循“尝试并在发生异常时撤回”模式，以在并发场景中序列化操作。

锁设计具有基于文件的两步式锁模式：`.lockFile` 用于标记锁状态，`.lockInfo` 用于记录锁的所有权：

1. 客户端在获取锁之前，首先检查是否存在有效（未过期）的 `.lockFile`。

2. 如果不存在，客户端会尝试访问存储在 `.lockInfo` 中的信息，以查看其自身是否持有锁；如果不存在有效的文件，客户端会尝试将其 UUID 写入到 `.lockInfo` 文件中。

3. 在执行所有这些操作之后，客户端会启动一个后台线程，以不断更新 `.lockFile`，从而告知其他客户端文件系统已锁定，然后更新文件系统。

4. 如果上述检查中的任一项表明锁已由其他客户端获取，或者在该过程中发生了任何异常，则当前进程/线程放弃（撤回）锁定，并在下一个循环中再次尝试。