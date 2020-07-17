
# ALTER TABLE

## 摘要

``` sql
ALTER TABLE name RENAME TO new_name
ALTER TABLE name ADD COLUMN column_name data_type [ COMMENT comment ] [ WITH ( property_name = expression [, ...] ) ]
ALTER TABLE name DROP COLUMN column_name
ALTER TABLE name RENAME COLUMN column_name TO new_column_name
```

## 说明

更改现有表的定义。

## 示例

将表 `users` 重命名为 `people`：

    ALTER TABLE users RENAME TO people;

在 `users` 表中添加 `zip` 列：

    ALTER TABLE users ADD COLUMN zip varchar;

从 `users` 表中删除 `zip` 列：

    ALTER TABLE users DROP COLUMN zip;

将 `users` 表中的 `id` 列重命名为 `user_id`。

    ALTER TABLE users RENAME COLUMN id TO user_id;

## 另请参见

[CREATE TABLE](./create-table.md)