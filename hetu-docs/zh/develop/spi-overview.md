
# SPI 概述

当您实现一个新的 openLooKeng 插件时，您将实现多个接口并覆盖 SPI 定义的方法。插件可以提供额外的[连接器](connectors.md)、[类型](types.md)、[函数](functions.md)和[系统访问控制](system-access-control.md)。特别地，连接器是用于在 openLooKeng 中进行查询的所有数据的源：它们支持 openLooKeng 可用的每个目录。

## 代码

可以在 openLooKeng 源代码树根目录下的 `presto-spi` 目录中找到 SPI 源代码。

## 插件元数据

每个插件标识一个入口点：`Plugin` 接口的实现。该类名通过标准 Java `ServiceLoader` 接口提供给 openLooKeng：classpath 包含 `META-INF/services` 目录中一个名为 `io.prestosql.spi.Plugin` 的资源文件。该文件的内容只有一行，其中列出了插件类的名称：

```
com.example.plugin.ExamplePlugin
```

对于包含在 openLooKeng 源代码中的内置插件，每当插件的 `pom.xml` 文件包含以下行时，就会创建该资源文件：

``` xml
<packaging>hetu-plugin</packaging>
```

## 插件

对于想要了解 openLooKeng SPI 的开发人员而言，`Plugin` 接口是一个很好的起点。该接口包含用于检索插件可以提供的各种类的访问方法。例如，`getConnectorFactories()` 方法是一个顶层函数，当 openLooKeng 可以创建连接器实例来支持目录时，openLooKeng 会调用该函数来检索 `ConnectorFactory`。`Type`、`ParametricType`、`Function`、`SystemAccessControl` 和 `EventListenerFactory` 对象具有类似的方法。

## 通过 Maven 构建插件

插件依赖于 openLooKeng 的 SPI：

``` xml
<dependency>
    <groupId>io.prestosql</groupId>
    <artifactId>presto-spi</artifactId>
    <scope>provided</scope>
</dependency>
```

由于 openLooKeng 在运行时提供来自 SPI 的类，因此插件不应在插件程序集中包含这些类，所以插件使用 Maven `provided` 范围。

openLooKeng 还提供了其他一些依赖项，包括 Slice 和 Jackson 注释。特别地，Jackson 用于序列化连接器句柄，因此插件必须使用 openLooKeng 提供的注释版本。

所有其他依赖项都基于插件自身实现所需的内容。插件加载在一个单独的类加载器中，以提供隔离并允许插件使用 openLooKeng 内部使用的库的不同版本。

有关示例 `pom.xml` 文件，请参见 openLooKeng 源代码树根目录下 `presto-example-http` 目录中的示例 HTTP 连接器。

## 部署自定义插件

要在 openLooKeng 安装中添加自定义插件，请在 openLooKeng 插件目录中为该插件创建一个目录，并且在该目录中添加此插件的所有必需 JAR 文件。例如，对于名为 `my-functions` 的插件，您应在 openLooKeng 插件目录中创建 `my-functions` 目录，并在该目录中添加相关的 JAR 文件。

默认情况下，插件目录是相对于 openLooKeng 安装目录的 `plugin` 目录，但可以使用配置变量 `catalog.config-dir` 来配置插件目录。为了使 openLooKeng 能够识别新插件，您必须重新启动 openLooKeng。

必须在 openLooKeng 集群的所有节点（协调节点和工作节点）上安装插件。