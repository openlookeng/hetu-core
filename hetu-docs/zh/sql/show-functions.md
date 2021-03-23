
# SHOW FUNCTIONS

## 摘要

``` sql
SHOW FUNCTIONS [ LIKE pattern [ ESCAPE 'escape_character' ] ]
```

## 说明

列出所有可以在查询中使用的函数，`LIKE` 子句可用于限制函数名称列表。

**注意**

*如果需要列出外部函数，请设置会话属性list_built_in_functions_only为false。*