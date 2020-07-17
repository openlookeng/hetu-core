
# SHOW ROLES

## 摘要

``` sql
SHOW [CURRENT] ROLES [ FROM catalog ]
```

## 说明

`SHOW ROLES` 列出 `catalog`（如果未指定 `catalog`，则为当前目录）中的所有角色。

`SHOW CURRENT ROLES` 列出已针对 `catalog`（如果未指定 `catalog`，则为当前目录）中的会话启用的角色。