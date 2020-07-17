
# GRANT ROLES

## 摘要

``` sql
GRANT role [, ...]
TO ( user | USER user | ROLE role) [, ...]
[ GRANTED BY ( user | USER user | ROLE role | CURRENT_USER | CURRENT_ROLE ) ]
[ WITH ADMIN OPTION ]
```

## 说明

将指定的角色授给当前目录中的指定主体。

如果指定了 `WITH ADMIN OPTION` 子句，则可以使用 `GRANT` 选项将角色授给用户。

为了使针对角色的 `GRANT` 语句成功执行，执行该语句的用户应具有管理员角色或者应对于给定的角色拥有 `GRANT` 选项。

可选的 `GRANTED BY` 子句可使指定的主体作为授予方来授给角色。如果未指定 `GRANTED BY` 子句，则当前用户作为授予方来授给角色。

## 示例

将角色 `bar` 授给用户 `foo`：

    GRANT bar TO USER foo;

将角色 `bar` 和 `foo` 授给用户 `baz` 和角色 `qux` 并使其具有管理员选项：

    GRANT bar, foo TO USER baz, ROLE qux WITH ADMIN OPTION;

## 限制

某些连接器不支持角色管理。有关更多详细信息，请参见连接器文档。

## 另请参见

[CREATE ROLE](./create-role.md)、[DROP ROLE](./drop-role.md)、[SET ROLE](./set-role.md)、[REVOKE ROLES](./revoke-roles.md)