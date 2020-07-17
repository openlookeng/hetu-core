

# 启发式索引器 - 开发人员

## 组件

### IndexerCommand

将索引器公开为一个可执行文件，接受输出文件系统和位置、索引类型、表、列和分区等参数。提供包含使用信息的帮助对话框。

### IndexerFactory

- 接受表、列、数据源、索引存储、索引类型等参数，创建一个针对相应的 Datasource、IndexStore 和 Index 进行配置的索引器。
- 将通过完全限定表名识别数据源类型。
- 对于 HDFS 等数据源，生成器还将查询表元数据并识别数据文件的格式（例如 ORC 和 Parquet），还创建相应的 Datasource。此外，还将从目录文件中读取配置值来识别输出 IndexStore。
- 设置指定的索引类型

### IndexWriter

- IndexWriter 会通过 IndexerFactory 将所需的 Datasource、IndexStore 和 Index 注入到其自身中。
- 它将调用 Datasource 来读取记录，让 Index 创建指定的索引，然后将其持久化到 IndexStore 中。

### IndexClient

- 帮助从 IndexStore 中读取 Index 文件。
- 允许删除和显示 Index 文件。

### DataSource

- 所有支持的数据源必须实现的接口。
- 这将允许我们插入任何数据源，只要该数据源实现了所需的操作（如读取拆分）即可。
- 每个实现都是一个插件。

### HiveDataSource

- Hive 表的 Datasource 实现。
- 该实现将检查表的元数据，然后将工作委托给 ORC 或 Parquet 数据源（当前仅支持 ORC）。

### HdfsOrcDataSource

- HDFS ORC 文件的 Datasource 实现
- 读取 ORC 文件并为文件中的每个带区创建一个拆分。

### IndexStore

- 表示用于存储索引的通用存储。
- 为每个要实现的存储提供抽象读取/写入方法。
- 每个实现都是一个插件。

### LocalIndexStore

- 支持将索引写入到本地文件系统中。

### HdfsIndexStore

- 支持将索引写入到 HDFS 中。

### Index

- 表示通用索引类型。
- 为每个要实现的索引类型提供抽象方法。
- 每个实现都是一个插件。

### BloomIndex

- 基于 BloomFilter 创建 Index。

### BitmapIndex

- 基于位图索引创建 Index。

### MinMaxIndex

- 该 Index 仅存储拆分中的最小值和最大值。