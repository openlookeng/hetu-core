# CREATE CUBE

## 使用方式

```sql
CREATE CUBE [ IF NOT EXISTS ]
cube_name ON table_name WITH (
   AGGREGATIONS = ( expression [, ...] ), GROUP = ( column_name [, ...] )
   [, ( property_name = expression [, ...] ) ] 
)
```

## 介绍

使用指定的组和聚合创建新的star-tree多维数据集。使用`insert-into-cube`插入数据。

如果表已存在，可选的`IF NOT EXISTS`子句将导致错误被隐藏。

可以使用可选的`property_name`标签来设置创建的多维数据集的属性。要列出所有可用的表属性，请运行以下查询：

    SELECT * FROM system.metadata.table_properties

## 示例

在`orders`上创建新多维数据集`orders_cube`：

    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), AVG(totalprice) ),
      GROUP = ( orderstatus, orderdate ),
      format = 'ORC'
    )

创建新的分区多维数据集`orders_cube`：

    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), AVG(totalprice) ),
      GROUP = ( orderstatus, orderdate ),
      format = 'ORC',
      partitioned_by = ARRAY['orderdate']
    )

## 限制

- 支持的聚合函数：COUNT、COUNT DISTINCT、MIN、MAX、SUM、AVG
- 每个多维数据集仅支持一个组。
- 不同的连接器可能支持不同的数据类型和不同的表/列属性。
- 当前只能在Hive连接器中创建多维数据集，但可以从另一个连接器在表中创建多维数据集。

## 另请参见

[INSERT INTO CUBE](./insert-cube.md)、[SHOW CUBES](./show-cubes.md)、[DROP CUBE](./drop-cube.md)