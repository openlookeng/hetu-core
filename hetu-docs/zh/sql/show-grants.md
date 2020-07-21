
# SHOW GRANTS

## 摘要

``` sql
SHOW GRANTS [ ON [ TABLE ] table_name ]
```

## 说明

列出当前用户对当前目录中指定表的权限。

如果未指定表名，该命令将列出当前用户对当前目录的所有模式中所有表的权限。

该命令要求设置当前目录。

**注意**

*在执行任何授权命令之前，确保已启用身份验证。*

## 示例

列出当前用户对表 `orders` 的权限：

    SHOW GRANTS ON TABLE orders;

列出当前用户对当前目录的所有模式中所有表的权限：

    SHOW GRANTS;

## 限制

某些连接器不支持 `SHOW GRANTS`。有关更多详细信息，请参见连接器文档。

## 另请参见

[GRANT](./grant.md)、[REVOKE](./revoke.md)