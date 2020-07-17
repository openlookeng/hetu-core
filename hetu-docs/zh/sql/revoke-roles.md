
# REVOKE ROLES

## 摘要

``` sql
REVOKE
[ ADMIN OPTION FOR ]
role [, ...]
FROM ( user | USER user | ROLE role) [, ...]
[ GRANTED BY ( user | USER user | ROLE role | CURRENT_USER | CURRENT_ROLE ) ]
```

## 说明

从当前目录中的指定主体回收指定的角色。

如果指定了 `ADMIN OPTION FOR` 子句，则回收 `GRANT` 权限，而不是回收角色。

为了使针对角色的 `REVOKE` 语句成功执行，执行该语句的用户应具有管理员角色或者应对于给定的角色拥有 `GRANT` 选项。

可选的 `GRANTED BY` 子句可使指定的主体作为回收方来回收角色。如果未指定 `GRANTED BY` 子句，则当前用户作为回收方来回收角色。

## 示例

从用户 `foo` 回收角色 `bar`：

    REVOKE bar FROM USER foo;

从用户 `baz` 和角色 `qux` 回收角色 `bar` 和 `foo` 的管理员选项：

    REVOKE ADMIN OPTION FOR bar, foo FROM USER baz, ROLE qux;

## 限制

某些连接器不支持角色管理。有关更多详细信息，请参见连接器文档。

## 另请参见

[CREATE ROLE](./create-role.md)、[DROP ROLE](./drop-role.md)、[SET ROLE](./set-role.md)、[GRANT ROLES](./grant-roles.md)