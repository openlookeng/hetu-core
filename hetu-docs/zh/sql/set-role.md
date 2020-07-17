
# SET ROLE

## 摘要

``` sql
SET ROLE ( role | ALL | NONE )
```

## 说明

`SET ROLE` 在当前目录中设置当前会话的启用角色。

`SET ROLE role` 启用为当前会话指定的单个角色。为了使 `SET ROLE role` 语句成功执行，执行该语句的用户应该具有给定角色的授予权限。

`SET ROLE ALL` 启用当前用户在当前会话中被授予的所有角色。

`SET ROLE NONE` 禁用当前用户在当前会话中被授予的所有角色。

## 限制

某些连接器不支持角色管理。有关更多详细信息，请参见连接器文档。

## 另请参见

[CREATE ROLE](./create-role.md)、[DROP ROLE](./drop-role.md)、[GRANT ROLES](./grant-roles.md)、[REVOKE ROLES](./revoke-roles.md)