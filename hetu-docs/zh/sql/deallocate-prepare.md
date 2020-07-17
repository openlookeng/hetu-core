
# DEALLOCATE PREPARE

## 摘要

``` sql
DEALLOCATE PREPARE statement_name
```

## 说明

从会话中的预编译语句列表中删除名称为 `statement_name` 的语句。

## 示例

取消分配名称为 `my_query` 的语句：

    DEALLOCATE PREPARE my_query;

## 另请参见

[PREPARE](./prepare.md)