
# EXECUTE

## 摘要

``` sql
EXECUTE statement_name [ USING parameter1 [ , parameter2, ... ] ]
```

## 说明

执行名称为 `statement_name` 的预编译语句。在 `USING` 子句中定义参数值。

## 示例

预编译并执行一个不具有参数的查询：

    PREPARE my_select1 FROM
    SELECT name FROM nation;

``` sql
EXECUTE my_select1;
```

预编译并执行一个具有两个参数的查询：

    PREPARE my_select2 FROM
    SELECT name FROM nation WHERE regionkey = ? and nationkey < ?;

``` sql
EXECUTE my_select2 USING 1, 3;
```

这等效于：

    SELECT name FROM nation WHERE regionkey = 1 AND nationkey < 3;

## 另请参见

[PREPARE](./prepare.md)