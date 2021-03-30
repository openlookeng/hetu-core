# SHOW CUBES

## 使用方式

```sql
SHOW CUBES [ FOR table_name ];
```

## 说明

`SHOW CUBES`列举所有多维数据集。添加选项`table_name`仅列出该表的多维数据集。

## 示例

显示所有多维数据集：

```sql
    SHOW CUBES;
```

显示`orders`表的多维数据集：

```sql
    SHOW CUBES FOR orders;
```

## 另请参见

[CREATE CUBE](./create-cube.md)、[DROP CUBE](./drop-cube.md)、[INSERT INTO CUBE](./insert-cube.md)