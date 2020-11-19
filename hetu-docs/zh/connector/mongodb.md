
# MongoDB连接器

MongoDB连接器允许将MongoDB集合作为openLooKeng中的表使用。

**注意**

    支持MongoDB 2.6+，但建议使用3.0或更高版本。

## 配置

要配置MongoDB连接器，参考以下内容创建目录属性文件`etc/catalog/mongodb.properties`，并根据需要替换这些属性：

``` properties
connector.name=mongodb
mongodb.seeds=host1,host:port
```

### 多个MongoDB集群

可以根据需要创建多个目录，因此，如果有额外的MongoDB集群，只需添加另一个不同的名称的属性文件到`etc/catalog`中（确保它以`.properties`结尾）。例如，如果将属性文件命名为`sales.properties`，openLooKeng将使用配置的连接器创建一个名为`sales`的目录。

## 配置属性

支持的配置属性：

| 属性名称| 说明|
|----------|----------|
| `mongodb.seeds`| 所有mongod服务器的列表|
| `mongodb.schema-collection`| 一个包含schema信息的集合|
| `mongodb.case-insensitive-name-matching`| 不区分大小写匹配数据库和集合名称|
| `mongodb.credentials`| 认证列表|
| `mongodb.min-connections-per-host`| 每个主机的最小连接池大小|
| `mongodb.connections-per-host`| 每个主机的连接池的最大大小|
| `mongodb.max-wait-time`| 最大等待时间|
| `mongodb.max-connection-idle-time`| 池化连接的最大空闲时间|
| `mongodb.connection-timeout`| 通信连接超时|
| `mongodb.socket-timeout`| 通信超时|
| `mongodb.socket-keep-alive`| 是否在每个通信上使能keep-alive|
| `mongodb.ssl.enabled`| 使用TLS/SSL连接到mongod/mongos|
| `mongodb.read-preference`| 读偏好|
| `mongodb.write-concern`| 写入策略|
| `mongodb.required-replica-set`| 所需的副本集名称|
| `mongodb.cursor-batch-size`| 批量返回的元素数|

### `mongodb.seeds`

同一副本集中所有mongod服务器列表，以逗号分隔 ，格式如``hostname[:port]``，或者同一个分片集群的mongos服务器列表。如果不指定port，则使用27017端口。

此属性是必需的；没有默认值，并且至少必须定义一个seed。

### `mongodb.schema-collection`

由于MongoDB是文档数据库，因此系统中没有固定的schema信息。因此，每个MongoDB数据库中的一个特殊集合应定义所有表的架构。有关详细信息，请参阅[表格定义](./mongodb.md#表格定义)部分。

在启动时，此连接器尝试猜测字段的类型，但可能与用户创建的集合可不匹配。在这种情况下，需要手动修改它。`CREATE TABLE`和`CREATE TABLE AS SELECT`会为用户创建一个条目。

该属性是可选的；默认值为`_schema`。

### `mongodb.case-insensitive-name-matching`

不区分大小写匹配数据库和集合名称。

该属性是可选的；默认值为`false`。

### `mongodb.credentials`

以逗号分隔的`username:password@collection`认证列表。

该属性是可选的；没有默认值。

### `mongodb.min-connectenions-per-host`

此MongoClient实例每个主机的最小连接数。这些连接在空闲时将保留在连接池中。当这些连接空闲时，它们将保存在连接池中，并且随着时间的推移，连接池将确保至少包含这个最小数目的连接。

该属性是可选的；默认值为`0`。

### `mongodb.connections-per-host`

此MongoClient实例每个主机允许的最大连接数。这些连接在空闲时将保留在连接池中。一旦连接池资源耗尽，任何需要连接的操作都将阻塞等待可用连接。

该属性是可选的；默认值为`100`。

### `mongodb.max-wait-time`

线程可以等待连接变为可用状态的最大等待时间（以毫秒为单位）。值`0`表示它不会等待。负值表示无限期地等待连接变为可用。

该属性是可选的；默认值为`120000`。

### `mongodb.connections-timeout`

连接超时（以毫秒为单位）。值`0`表示没有超时。仅在建立新连接时使用。

该属性是可选的；默认值为`10000`。

### `mongodb.socket-timeout`

套接字超时（以毫秒为单位）。它用于I / O套接字读取和写入操作。

该属性是可选的；默认值为`0`并且表示没有超时。

### `mongodb.socket-keep-alive`

此标志控制套接字保持活动功能，该功能通过防火墙保持连接活动。

该属性是可选的；默认值为`false`。

### `mongodb.ssl.enabled`

此标志用于启用与MongoDB服务器的SSL连接。

该属性是可选的；默认值为`false`。

### `mongodb.read-preference`

读偏好用于查询、映射还原、聚合和计数。可配置的值有`PRIMARY`，`PRIMARY_PREFERRED`，`SECONDARY`，`SECONDARY_PREFERRED`和`NEAREST`。

该属性是可选的；默认值为`PRIMARY`。

### `mongodb.write-concern`

写入策略。可配置的值有`ACKNOWLEDGED`，`FSYNC_SAFE`，`FSYNCED`，`JOURNAL_SAFEY`，`JOURNALED`，`MAJORITY`，`NORMAL`，`REPLICA_ACKNOWLEDGED`，`REPLICAS_SAFE`和`UNACKNOWLEDGED`。

该属性是可选的；默认值为`ACKNOWLEDGED`。

### `mongodb.required-replica-set`

所需的副本集名称。设置此选项后，MongoClient实例将执行以下操作:

>- 以副本集模式连接，并根据给定的服务器发现集合中的所有成员
>- 确保所有成员报告的集合名称与所需的集合名称匹配。
>- 如果seed列表的任何成员不是具有必需名称的副本集的一部分，则拒绝处理任何请求。

该属性是可选的。没有默认值。

### `mongodb.cursor-batch-size`

限制一批中返回的元素数。游标通常会获取一批结果对象并将其存储在本地。
如果batchSize为0，将使用驱动程序的默认值。
如果batchSize为正，则其值表示检索到的每批对象的大小。可以对其进行调整以优化性能并限制数据传输。
如果batchSize为负，它将限制返回的对象数量，这些对象的数量在最大批处理大小限制内（通常为4MB），并且游标将被关闭。例如，如果batchSize为-10，则服务器将最多返回10个文档，并尽可能多地返回4MB中可以容纳的文档，然后关闭游标。

**注意**

    请勿将批量大小设置为1。

该属性是可选的；默认值为`0`。

## 表格定义

MongoDB在`mongodb.schema-collection`指定的配置特殊集合上维护表格定义。

**注意**

    插件无法检测到集合被删除。
    需要在Mongo shell中通过db.getCollection("_schema").remove({ table: delete_table_name })删除集合。
    或者通过使用openLooKeng运行DROP TABLE table_name来删除集合。

schema中的集合由表的MongoDB文档组成。

    {
        "table": ...,
        "fields": [
              { "name" : ...,
                "type" : "varchar|bigint|boolean|double|date|array(bigint)|...",
                "hidden" : false },
                ...
            ]
        }
    }

| 字段| 是否必填| 类型| 说明|
|:----------|:----------|:----------|:----------|
| `table`| 必填| string| openLooKeng表名称。|
| `fields`| 必填| array| 字段定义列表。每个字段定义在openLooKeng表中创建一个新列。|

每个字段定义：

    {
        "name": ...,
        "type": ...,
        "hidden": ...
    }

| 字段| 是否必填| 类型| 说明|
|:----------|:----------|:----------|:----------|
| `name`| 必填| string| openLooKeng表中的列名。|
| `type`| 必填| string| 列的类型。|
| `hidden`| 可选| boolean| 从`DESCRIBE <table name>`和`SELECT *`中隐藏该列。默认为`false`。|

密钥或消息的字段描述没有限制。

## ObjectId

MongoDB集合具有特殊字段`_id`。连接器尝试对此特殊字段遵循相同的规则，因此将存在隐藏字段`_id`。
```sql
    CREATE TABLE IF NOT EXISTS orders (
        orderkey bigint,
        orderstatus varchar,
        totalprice double,
        orderdate date
    );

    INSERT INTO orders VALUES(1, 'bad', 50.0, current_date);
    INSERT INTO orders VALUES(2, 'good', 100.0, current_date);
    SELECT _id, * FROM orders;
```
```sql
                     _id                 | orderkey | orderstatus | totalprice | orderdate
    -------------------------------------+----------+-------------+------------+------------
     55 b1 51 63 38 64 d6 43 8c 61 a9 ce |        1 | bad         |       50.0 | 2015-07-23
     55 b1 51 67 38 64 d6 43 8c 61 a9 cf |        2 | good        |      100.0 | 2015-07-23
    (2 rows)
```
```sql
    SELECT _id, * FROM orders WHERE _id = ObjectId('55b151633864d6438c61a9ce');
```
```sql
                     _id                 | orderkey | orderstatus | totalprice | orderdate
    -------------------------------------+----------+-------------+------------+------------
     55 b1 51 63 38 64 d6 43 8c 61 a9 ce |        1 | bad         |       50.0 | 2015-07-23
    (1 row)
```

可以将`_id`字段呈现为可读值，并转换为`VARCHAR`：

```sql
    SELECT CAST(_id AS VARCHAR), * FROM orders WHERE _id = ObjectId('55b151633864d6438c61a9ce');
```
```sql

               _id             | orderkey | orderstatus | totalprice | orderdate
    ---------------------------+----------+-------------+------------+------------
     55b151633864d6438c61a9ce  |        1 | bad         |       50.0 | 2015-07-23
    (1 row)
```

限制
-----------
>- 不支持使用[删除行](../sql/delete.md)
>- 不支持视图创建
>- MongoDB查询不到openLooKeng新建的空表和字段。
>- MongoDB删除表、字段后，openLooKeng中表格或者字段仍存在，但是值为NULL。






