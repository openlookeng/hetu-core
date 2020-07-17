
# DESCRIBE OUTPUT

## 摘要

``` sql
DESCRIBE OUTPUT statement_name
```

## 说明

列出预编译语句的输出列，包括列名（或别名）、目录、模式、表、类型、类型大小（以字节为单位）以及表明列是否为别名的布尔值。

## 示例

预编译并描述一个具有四个输出列的查询：

    PREPARE my_select1 FROM
    SELECT * FROM nation

``` sql
DESCRIBE OUTPUT my_select1;
```

```
Column Name | Catalog | Schema | Table  |  Type   | Type Size | Aliased
-------------+---------+--------+--------+---------+-----------+---------
nationkey   | tpch    | sf1    | nation | bigint  |         8 | false
name        | tpch    | sf1    | nation | varchar |         0 | false
regionkey   | tpch    | sf1    | nation | bigint  |         8 | false
comment     | tpch    | sf1    | nation | varchar |         0 | false
(4 rows)
```

预编译并描述一个输出列是表达式的查询：

    PREPARE my_select2 FROM
    SELECT count(*) as my_count, 1+2 FROM nation

``` sql
DESCRIBE OUTPUT my_select2;
```

```
Column Name | Catalog | Schema | Table |  Type  | Type Size | Aliased
-------------+---------+--------+-------+--------+-----------+---------
my_count    |         |        |       | bigint |         8 | true
_col1       |         |        |       | bigint |         8 | false
(2 rows)
```

预编译并描述一个行计数查询：

    PREPARE my_create FROM
    CREATE TABLE foo AS SELECT * FROM nation

``` sql
DESCRIBE OUTPUT my_create;
```

```
Column Name | Catalog | Schema | Table |  Type  | Type Size | Aliased
-------------+---------+--------+-------+--------+-----------+---------
rows        |         |        |       | bigint |         8 | false
(1 row)
```

## 另请参见

[PREPARE](./prepare.md)