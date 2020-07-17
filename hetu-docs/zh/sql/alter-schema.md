
# ALTER SCHEMA

## 摘要

```sql
ALTER {SCHEMA|DATABASE} name RENAME TO new_name
```

## 说明

更改现有模式的定义。

## 示例

将模式 `web` 重命名为 `traffic`：

    ALTER SCHEMA web RENAME TO traffic
    ALTER DATABASE web RENAME TO traffic

## 限制

某些连接器（如 Hive 连接器）不支持重命名模式。有关更多详细信息，请参见连接器文档。

## 另请参见

[CREATE SCHEMA](./create-schema.md)