
# GRANT

## 摘要

``` sql
GRANT ( privilege [, ...] | ( ALL PRIVILEGES ) )
ON [ TABLE ] table_name TO ( user | USER user | ROLE role )
[ WITH GRANT OPTION ]
```

## 说明

将指定的权限授给指定的被授权者。

指定 `ALL PRIVILEGES` 可以授予 [DELETE](./delete.md)、[INSERT](./insert.md) 和 [SELECT](./select.md) 权限。

指定 `ROLE PUBLIC` 可以将权限授给 `PUBLIC` 角色，从而将权限授给所有用户。

通过使用可选的 `WITH GRANT OPTION` 子句，可以允许被授权者将同样的权限授给其他用户。

为了使 `GRANT` 语句成功执行，执行该语句的用户应拥有指定的权限并且对于这些权限拥有 `GRANT OPTION`。

## 示例

将对表 `orders` 的 `INSERT` 和 `SELECT` 权限授给用户 `alice`：

    GRANT INSERT, SELECT ON orders TO alice;

将对表 `nation` 的 `SELECT` 权限授给用户 `alice`，此外允许 `alice` 将 `SELECT` 权限授给其他用户：

    GRANT SELECT ON nation TO alice WITH GRANT OPTION;

将对表 `orders` 的 `SELECT` 权限授给所有用户：

    GRANT SELECT ON orders TO ROLE PUBLIC;

## 限制

某些连接器不支持 `GRANT`。有关更多详细信息，请参见连接器文档。

## 另请参见

[REVOKE](./revoke.md)、[SHOW GRANTS](./show-grants.md)