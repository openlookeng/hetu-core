
# 系统连接器

系统连接器提供当前运行的openLooKeng集群的信息和指标。这通过正常SQL查询实现。

## 配置

系统连接器无需配置：它通过一个名为`system`的目录自动可用。

## 使用系统连接器

列出可用的系统模式：

    SHOW SCHEMAS FROM system;

列出其中一个模式中的表：

    SHOW TABLES FROM system.runtime;

查询其中一个表：

    SELECT * FROM system.runtime.nodes;

杀掉正在运行的查询：

    CALL system.runtime.kill_query(query_id => '20151207_215727_00146_tx3nr', message => 'Using too many resources');

## 系统连接器表

### `metadata.catalogs`

catalogs包含可用的目录列表。

### `metadata.schema_properties`

schema properties表包含可在创建新模式时设置的可用属性列表。

### `metadata.table_properties`

table properties表包含可在创建新表时设置的可用属性列表。

### `metadata.table_comments`

table comments表包含表注释的列表。

### `runtime.nodes`

nodes表包含openLooKeng集群中可见的节点列表及其状态。

### `runtime.queries`

queries表包含当前和最近运行的对openLooKeng集群查询的信息。从这个表中可以找到原始查询文本（SQL）、运行查询的用户的标识以及查询的性能信息，包括查询的排队和分析时间。

### `runtime.tasks`

tasks表包含有关openLooKeng查询中涉及的任务的信息，包括这些任务的执行位置以及每个任务处理的行数和字节数。

### `runtime.transactions`

transactions表包含当前打开的事务和相关元数据的列表。这包括例如创建时间、空闲时间、初始化参数和访问的目录等信息。

## 系统连接器流程

**runtime.kill\_query(query\_id, message)**

杀掉由`query_id`所标识的查询。查询失败消息中会包含指定的`message`。