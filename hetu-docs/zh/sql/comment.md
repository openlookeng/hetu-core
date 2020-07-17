
# COMMENT

## 摘要

``` sql
COMMENT ON TABLE name IS 'comments'
```

## 说明

设置表的注释。可以通过将注释设置为 `NULL` 来删除注释。

## 示例

将 `users` 表的注释更改为 `master table`：

    COMMENT ON TABLE users IS 'master table';