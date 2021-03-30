# INSERT INTO CUBE

## 使用方式

```sql
INSERT INTO CUBE cube_name [WHERE condition]
```

## 介绍

将数据插入star-tree多维数据集。谓词信息为可选项。如果提供了谓词，则仅从源表处理与给定谓词匹配的数据并将其插入多维数据集。否则，将处理源表中的全部数据并将其插入多维数据集。

## 示例

根据条件将数据插入`orders_cube`多维数据集中：

    INSERT INTO CUBE orders_cube WHERE orderdate > date '1999-01-01';
    INSERT INTO CUBE order_all_cube;

## 另请参见

[INSERT OVERWRITE CUBE](./insert-overwrite-cube.md)、[CREATE CUBE](./create-cube.md)、[SHOW CUBES](./show-cubes.md)、[DROP CUBE](./drop-cube.md)

## 限制

1. 如果在where子句谓词中使用两个不同的列，则insert语句将无法正常运行。

```sql
   CREATE CUBE orders_cube ON orders WITH (AGGREGATIONS = (count(*)), GROUP = (orderdate));
   
   INSERT INTO CUBE orders_cube WHERE orderdate BETWEEN date '1999-01-01' AND date '1999-01-05';
   
   -- This statement would fail because its possible the Cube already contain rows matching the given predicate.
   INSERT INTO CUBE orders_cube WHERE location = 'Canada';
```

2. 范围谓词问题。

```sql
   CREATE CUBE orders_cube ON orders WITH (AGGREGATIONS = (count(*)), GROUP = (orderdate));
   
   INSERT INTO CUBE orders_cube WHERE orderdate BETWEEN date '1999-01-01' AND date '1999-01-05';
   INSERT INTO CUBE orders_cube WHERE orderdate BETWEEN date '1999-01-06' AND date '1999-01-10';
   
   SET SESSION enable_star_tree_index=true;
   
   -- This statement uses orders_cube --
   SELECT count(*) FROM orders WHERE orderdate BETWEEN date '1999-01-03' AND date '1999-01-04';
   -- This statement uses orders_cube --
   SELECT count(*) FROM orders WHERE orderdate BETWEEN date '1999-01-07' AND date '1999-01-09';
   -- This statement does not user orders_cube because the two predicates used in the INSERT statement cannot be merged 
   -- and the TupleDomain evaluation to check cubePredicate.contains(statementPredicate) evaluates to false
   
   SELECT count(*) FROM orders WHERE orderdate BETWEEN date '1999-01-04' AND date '1999-01-07';
```