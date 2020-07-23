
# 从Hive迁移

openLooKeng使用ANSI SQL语法和语义，而Hive使用一种类似SQL的语言HiveQL。

## 使用下标代替UDF访问数组的动态索引

SQL中的下标运算符支持全表达式，而Hive只支持常量。因此，你可以如下编写查询：

```sql
SELECT my_array[CARDINALITY(my_array)] as last_element
FROM ...
```

## 避免越界访问数组

越界访问数组元素将导致异常。你可以使用`if`通过以下方式避免此情况：

```sql
SELECT IF(CARDINALITY(my_array) >= 3, my_array[3], NULL)
FROM ...
```

## 对数组使用ANSI SQL语法

数组的索引从1开始，而不是从0开始：

```sql
SELECT my_array[1] AS first_element
FROM ...
```

使用ANSI语法构造数组：

```sql
SELECT ARRAY[1, 2, 3] AS my_array
```

## 对标识符和字符串使用ANSI SQL语法

字符串用单引号分隔，标识符用双引号而不是反引号分隔：

```sql
SELECT name AS "User Name"
FROM "7day_active"
WHERE name = 'foo'
```

## 引用以数字开头的标识符

以数字开头的标识符在ANSI SQL中是不合法的，必须使用双引号括起来：

```sql
SELECT *
FROM "7day_active"
```

## 使用标准字符串连接运算符

使用ANSI SQL字符串连接运算符：

```sql
SELECT a || b || c
FROM ...
```

## 使用CAST目标的标准类型

`CAST`目标支持以下标准类型：

```sql
SELECT
  CAST(x AS varchar)
, CAST(x AS bigint)
, CAST(x AS double)
, CAST(x AS boolean)
FROM ...
```

特别地，使用`VARCHAR`代替`STRING`。

## 使用CAST进行整数除法

openLooKeng在对两个整数做除法时遵循执行整数除法的标准行为。例如，`7`除以`2`结果是`3`，而不是`3.5`。要对两个整数进行浮点除法，将其中一个转换为double：

```sql
SELECT CAST(5 AS DOUBLE) / 2
```

## 复杂表达式或查询使用WITH

当希望将复杂的输出表达式重新用作筛选器时，可以使用内联子查询，也可以使用`WITH`子句将其分解出来：

```sql
WITH a AS (
  SELECT substr(name, 1, 3) x
  FROM ...
)
SELECT *
FROM a
WHERE x = 'foo'
```

## 使用UNNEST来扩展数组和映射

openLooKeng支持`unnest`用于扩展数组和映射。使用`UNNEST`代替`LATERAL VIEW explode()`。

Hive查询：

```sql
SELECT student, score
FROM tests
LATERAL VIEW explode(scores) t AS score;
```

openLooKeng查询：

```sql
SELECT student, score
FROM tests
CROSS JOIN UNNEST(scores) AS t (score);
```