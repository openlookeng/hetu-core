
# UPDATE

## 摘要

``` sql
UPDATE table_name SET column_name = expression[, column_name = expression, ... ] [ WHERE condition ]
```

## 说明

`UPDATE` 更改满足条件的所有行中指定列的值。只需在 SET 子句中涉及要修改的列；未显式修改的列保持原来的值。

## 示例

更新表 `users`，将 `id` 等于 1 的行中的姓名更改为 `Francisco`：

``` sql
    UPDATE users SET name = 'Francisco' WHERE id=1;
```

## 限制

- 目前只有 Hive 连接器和事务 ORC 表支持`UPDATE`。
- SET 表达式不支持子查询。
- 支持直接列引用，但不支持带列引用的表达式。
- 无法将 `UPDATE` 应用于视图。
- `UPDATE` 不支持隐式数据类型转换，当值与目标列的数据类型不匹配时，请使用 `CAST`。
- 如果表进行了分区和/或分桶，则无法更新 bucket 列和 partition 列。也就是说，它们不能是 SET 表达式的目标。

## 另请参见

[INSERT](./insert.md)、[INSERT OVERWRITE](./insert-overwrite.md)