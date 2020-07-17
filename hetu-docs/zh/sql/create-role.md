
# CREATE ROLE

## 摘要

``` sql
CREATE ROLE role_name
[ WITH ADMIN ( user | USER user | ROLE role | CURRENT_USER | CURRENT_ROLE ) ]
```

## 说明

`CREATE ROLE` 在当前目录中创建指定的角色。

如果使用可选的 `WITH ADMIN` 子句，则在创建角色时使指定的用户成为角色管理员。角色管理员具有删除或授予角色的权限。如果未指定可选的 `WITH ADMIN` 子句，在创建角色时使当前用户成为角色管理员。

## 示例

创建角色 `admin`：

    CREATE ROLE admin;

创建角色 `moderator` 并使 `bob` 成为角色管理员：

    CREATE ROLE moderator WITH ADMIN USER bob;

## 限制

某些连接器不支持角色管理。有关更多详细信息，请参见连接器文档。

## 另请参见

[DROP ROLE](./drop-role.md)、[SET ROLE](./set-role.md)、[GRANT ROLES](./grant-roles.md)、[REVOKE ROLES](./revoke-roles.md)