
# DROP ROLE

## 摘要

``` sql
DROP ROLE role_name
```

## 说明

`DROP ROLE` 删除当前目录中的指定角色。

为了使 `DROP ROLE` 语句成功执行，执行该语句的用户应拥有给定角色的管理员权限。

## 示例

删除角色 `admin`：

    DROP ROLE admin;

## 限制

某些连接器不支持角色管理。有关更多详细信息，请参见连接器文档。

## 另请参见

[CREATE ROLE](./create-role.md)、[SET ROLE](./set-role.md)、[GRANT ROLES](./grant-roles.md)、[REVOKE ROLES](./revoke-roles.md)