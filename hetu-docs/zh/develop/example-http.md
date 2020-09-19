
# Example HTTP 连接器

Example HTTP 连接器有一个简单目标：通过 HTTP 读取逗号分隔的数据。例如，如果您有大量 CSV 格式的数据，您可以将 Example HTTP 连接器指向这些数据并编写 SQL 查询来处理这些数据。

## 代码

可以在 openLooKeng 源代码树根目录下的 `presto-example-http` 目录中找到 Example HTTP 连接器。

## 插件实现

Example HTTP 连接器中的插件实现与其他插件实现非常相似。大部分实现都聚焦于处理可选配置，唯一相关的函数如下所示：

``` java
@Override
public Iterable<ConnectorFactory> getConnectorFactories()
{
    return ImmutableList.of(new ExampleConnectorFactory());
}
```

请注意，`ImmutableList` 类是 Guava 提供的一个实用程序类。

与所有连接器一样，该插件会覆盖 `getConnectorFactories()` 方法并返回一个 `ExampleConnectorFactory`。

## ConnectorFactory Implementation

在 openLooKeng 中，处理 openLooKeng 与特定类型数据源之间连接的主要对象是 `Connector` 对象，该对象是使用 `ConnectorFactory` 创建的。

类 `ExampleConnectorFactory` 中提供了该实现。连接器工厂实现所做的第一件事是指定该连接器的名称。这是用于在 openLooKeng 配置中引用此连接器的同一字符串。

``` java
@Override
public String getName()
{
    return "example-http";
}
```

连接器工厂的实际工作在 `create()` 方法中执行。在 `ExampleConnectorFactory` 类中，`create()` 方法配置连接器，然后请求 Guice 创建对象。以下是不含参数校验和异常处理的 `create()` 方法的内容：

``` java
// A plugin is not required to use Guice; it is just very convenient
Bootstrap app = new Bootstrap(
        new JsonModule(),
        new ExampleModule(catalogName));

Injector injector = app
        .strictConfig()
        .doNotInitializeLogging()
        .setRequiredConfigurationProperties(requiredConfig)
        .initialize();

return injector.getInstance(ExampleConnector.class);
```

### 连接器：ExampleConnector

该类使 openLooKeng 能够获取对连接器提供的各种服务的引用。

### 元数据：ExampleMetadata

该类负责报告表名、表元数据、列名、列元数据以及有关该连接器提供的模式的其他信息。openLooKeng 还调用 `ConnectorMetadata` 来确保特定的连接器能够理解和处理给定的表名。

`ExampleMetadata` 实现将许多此类调用委托给 `ExampleClient`，该类可以实现连接器的大部分核心功能。

### 分片管理器： ExampleSplitManager

分片管理器将表的数据分区成多个块，这些块由 openLooKeng 分发至工作节点进行处理。对于 Example HTTP 连接器，每个表都包含一个或多个指向实际数据的 URI。系统针对每个 URI 创建一个分片。

### 记录集提供程序：ExampleRecordSetProvider

记录集提供程序创建一个记录集，后者又相应地创建一个向 openLooKeng 返回实际数据的记录游标。`ExampleRecordCursor` 通过 HTTP 从 URI 读取数据。每一行对应于单个记录行。行通过逗号拆分为各个字段值，然后这些字段值返回至 openLooKeng。