
# DESCRIBE INPUT

## 摘要

``` sql
DESCRIBE INPUT statement_name
```

## 说明

列出预编译语句的输入参数以及每个参数的位置和类型。无法确定的参数类型将显示为 `unknown`。

## 示例

预编译并描述一个具有三个参数的查询：

``` sql
PREPARE my_select1 FROM
SELECT ? FROM nation WHERE regionkey = ? AND name < ?;
```

``` sql
DESCRIBE INPUT my_select1;
```

``` sql
Position | Type
--------------------
       0 | unknown
       1 | bigint
       2 | varchar
(3 rows)
```

预编译并描述一个不具有参数的查询：

``` sql
PREPARE my_select2 FROM
SELECT * FROM nation;
```

``` sql
DESCRIBE INPUT my_select2;
```

``` sql
Position | Type
-----------------
(0 rows)
```

## 另请参见

[PREPARE](./prepare.md)