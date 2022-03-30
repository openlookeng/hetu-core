
# 连接器

连接器是用于在 openLooKeng 中进行查询的所有数据的源。即使您的数据源没有可以支持它的基础表，只要将您的数据源与 openLooKeng 期望使用的 API 相适配，您也可以针对这些数据编写查询。

## ConnectorFactory

连接器的实例由 ``ConnectorFactory`` 实例创建，当 openLooKeng 在插件上调用 ``getConnectorFactory()`` 时会创建该 ``ConnectorFactory`` 实例。

连接器工厂是负责创建 ``Connector`` 对象实例的简单接口，可返回以下服务的实例：



* ``ConnectorMetadata``
* ``ConnectorSplitManager``
* ``ConnectorHandleResolver``
* ``ConnectorRecordSetProvider``

### ConnectorMetadata

连接器元数据接口具有许多重要的方法，这些方法负责允许 openLooKeng 查看模式列表、表列表、列列表以及有关特定数据源的其他元数据。




该接口太大，无法在文档中列出，不过如果您有兴趣查看实现这些方法的策略，请查看 `example-http` 和 Cassandra 连接器。

如果您的基础数据源支持模式、表和列，则该接口应该很容易实现。

如果您试图修改某些非关系数据库的内容（与 Example HTTP 连接器执行的操作类似），则可能需要创造性地将数据源映射到 openLooKeng 的模式、表和列概念。



### ConnectorSplitManager

分片管理器将表的数据分区成多个块，这些块由 openLooKeng 分发至工作节点进行处理。

例如，Hive 连接器列出每个 Hive 分区的文件，并为每个文件创建一个或多个分片。

对于没有已分区数据的数据源，此处一个比较好的策略是仅针对整个表返回单个分片。
这是 Example HTTP 连接器使用的策略。

### ConnectorRecordSetProvider

在给定一个分片和一个列列表的情况下，记录集提供程序负责将数据提供给 openLooKeng 执行引擎。

记录集提供程序创建一个 ``RecordSet``，后者又相应地创建一个 ``RecordCursor``，openLooKeng 使用该 ``RecordCursor`` 来读取每行的列值。

