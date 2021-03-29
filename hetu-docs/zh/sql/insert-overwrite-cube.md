# INSERT INTO CUBE

## 使用方式

```sql
INSERT OVERWRITE CUBE cube_name [WHERE condition]
```

## 介绍

类似于INSERT INTO CUBE语句，但使用此语句将覆盖现有数据。谓词为可选项。

## 示例

根据条件将数据插入`orders_cube`多维数据集中：

    INSERT OVERWRITE CUBE orders_cube WHERE orderdate > date '1999-01-01';
    INSERT OVERWRITE CUBE orders_cube;

## 另请参见

[INSERT INTO CUBE](./insert-cube.md)、[CREATE CUBE](./create-cube.md)、[SHOW CUBES](./show-cubes.md)、[DROP CUBE](./drop-cube.md)