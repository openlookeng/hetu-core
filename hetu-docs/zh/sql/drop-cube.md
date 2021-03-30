# DROP CUBE

## 使用方式

```sql
DROP CUBE  [ IF EXISTS ] cube_name
```

## 说明

删除已存在的多维数据集。

如果多维数据集不存在，可选子句`IF EXISTS`将导致错误被隐藏。

## 示例

删除多维数据集`orders_cube`：

    DROP CUBE orders_cube

如果多维数据集`orders_cube`存在，则将其删除：

    DROP CUBE IF EXISTS orders_cube

## 另请参见

[CREATE CUBE](./create-cube.md)、[SHOW CUBES](./show-cubes.md)、[INSERT INTO CUBE](./insert-cube.md)