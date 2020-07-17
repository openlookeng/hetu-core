
# CREATE TABLE

## 摘要

``` sql
CREATE TABLE [ IF NOT EXISTS ]
table_name (
  { column_name data_type [ COMMENT comment ] [ WITH ( property_name = expression [, ...] ) ]
  | LIKE existing_table_name [ { INCLUDING | EXCLUDING } PROPERTIES ] }
  [, ...]
)
[ COMMENT table_comment ]
[ WITH ( property_name = expression [, ...] ) ]
```

## 说明

创建一个具有指定列的空表。使用 `create-table-as` 可以创建含数据的表。

如果使用可选的 `IF NOT EXISTS` 子句，则在表已存在时禁止显示错误。

可以使用可选的 `WITH` 子句来设置创建的表或单个列的属性。要列出所有可用的表属性，请运行以下查询：

    SELECT * FROM system.metadata.table_properties

例如，对于 Hive 连接器，以下是一些可用且常用的表属性：

| 属性名称         | 数据类型       | 说明                                                         | 默认值  |
| ---------------- | -------------- | ------------------------------------------------------------ | ------- |
| `format`         | varchar        | 表的 Hive 存储格式。可能的值为：ORC、PARQUET、AVRO、RCBINARY、RCTEXT、SEQUENCEFILE、JSON、TEXTFILE 和 CSV。 | ORC     |
| `bucket_count`   | integer        | 桶的数量。                                                   |         |
| `bucketed_by`    | array(varchar) | 分桶列。                                                     |         |
| `sorted_by`      | array(varchar) | 桶排序列。                                                   |         |
| `external`       | boolean        | 表是否为外部表。                                             | `false` |
| `location`       | varchar        | 表的文件系统位置 URI。如果 `external`=`true`，则必须提供位置值。 |         |
| `partitioned_by` | array(varchar) | 分区列。                                                     |         |
| `transactional`  | boolean        | 是否启用事务属性。存在一个限制，即仅 ORC 存储格式支持创建事务表。 | `false` |

要列出所有可用的列属性，请运行以下查询：

    SELECT * FROM system.metadata.column_properties

可以使用 `LIKE` 子句在新表中包含现有表中的所有列定义。可以指定多个 `LIKE` 子句，从而允许复制多个表中的列。

如果指定了 `INCLUDING PROPERTIES`，则将所有表属性复制到新表中。如果 `WITH` 子句指定的属性名称与某个复制的属性的名称相同，则使用 `WITH` 子句中的值。默认行为是 `EXCLUDING PROPERTIES`。最多只能为一个表指定 `INCLUDING PROPERTIES` 选项。

## 示例

创建表 `orders`：

    CREATE TABLE orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double,
      orderdate date
    )
    WITH (format = 'ORC')

创建事务表 `orders`：

    CREATE TABLE orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double,
      orderdate date
    )
    WITH (format = 'ORC',
    transactional=true)

创建外部表 `orders`：

    CREATE TABLE orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double,
      orderdate date
    )
    WITH (format = 'ORC',
    external=true,
    location='hdfs://hdcluster/tmp/externaltbl')

如果表 `orders` 不存在，则创建该表，同时添加表注释和列注释：

    CREATE TABLE IF NOT EXISTS orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double COMMENT 'Price in cents.',
      orderdate date
    )
    COMMENT 'A table to keep track of orders.'

使用 `orders` 中的列并在开头和结尾使用附加的列创建表 `bigger_orders`：

    CREATE TABLE bigger_orders (
      another_orderkey bigint,
      LIKE orders,
      another_orderdate date
    )

## 限制

不同的连接器可能支持不同的数据类型和不同的表/列属性。有关更多详细信息，请参见连接器文档。

## 另请参见

[ALTER TABLE](./alter-table.md)、[DROP TABLE](./drop-table.md)、[CREATE TABLE AS](./create-table-as.md)、[SHOW CREATE TABLE](./show-create-table.md)