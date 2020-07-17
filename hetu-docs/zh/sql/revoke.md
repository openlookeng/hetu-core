
# REVOKE

## 摘要

``` sql
REVOKE [ GRANT OPTION FOR ]
( privilege [, ...] | ALL PRIVILEGES )
ON [ TABLE ] table_name FROM ( user | USER user | ROLE role )
```

## 说明

从指定的被授权者回收指定的权限。

指定 `ALL PRIVILEGES` 可以回收 [DELETE](./delete.md)、[INSERT](./insert.md) 和 [SELETE](./select.md) 权限。

指定 `ROLE PUBLIC` 可以从 `PUBLIC` 角色回收权限。用户将保留直接或通过其他角色分配给他们的权限。

可选的 `GRANT OPTION FOR` 子句还会回收授予指定权限的权限。

为了使 `REVOKE` 语句成功执行，执行该语句的用户应拥有指定的权限并且对于这些权限拥有 `GRANT OPTION`。

## 示例

从用户 `alice` 回收对表 `orders` 的 `INSERT` 和 `SELECT` 权限：

    REVOKE INSERT, SELECT ON orders FROM alice;

从所有用户回收对表 `nation` 的 `SELECT` 权限，此外还回收授予 `SELECT` 权限的权限：

    REVOKE GRANT OPTION FOR SELECT ON nation FROM ROLE PUBLIC;

从用户 `alice` 回收对表 `test` 的所有权限：

    REVOKE ALL PRIVILEGES ON test FROM alice;

## 限制

某些连接器不支持 `REVOKE`。有关更多详细信息，请参见连接器文档。

## 另请参见

[GRANT](./grant.md)、[SHOW GRANTS](./show-grants.md)