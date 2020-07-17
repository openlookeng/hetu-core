
# 条件表达式

## CASE

标准 SQL `CASE` 表达式具有两种形式。“简单”形式从左向右搜索每个 `value` 表达式，直到找到一个等于 `expression` 的表达式：

``` sql
CASE expression
    WHEN value THEN result
    [ WHEN ... ]
    [ ELSE result ]
END
```

会返回匹配的 `value` 的 `result`。如果没有找到任何匹配项，那么如果存在 `ELSE` 子句，则返回该子句中的 `result`，否则返回 NULL。示例：

    SELECT a,
           CASE a
               WHEN 1 THEN 'one'
               WHEN 2 THEN 'two'
               ELSE 'many'
           END

“搜索”形式从左向右计算每个 boolean `condition` 的值，直到其中一个为 true 并返回匹配的 `result`：

``` sql
CASE
    WHEN condition THEN result
    [ WHEN ... ]
    [ ELSE result ]
END
```

如果没有任何条件为 true，那么如果存在 `ELSE` 子句，则返回该子句中的 `result`，否则返回 NULL。示例：

    SELECT a, b,
           CASE
               WHEN a = 1 THEN 'aaa'
               WHEN b = 2 THEN 'bbb'
               ELSE 'ccc'
           END

## IF

`IF` 函数实际上是一个语言构造，该函数等效于下面的 `CASE` 表达式：

> ``` sql
> CASE
>     WHEN condition THEN true_value
>     [ ELSE false_value ]
> END
> ```

**if(condition, true\_value)**

如果 `condition` 为 true，则计算并返回 `true_value`，否则返回 NULL 并且不计算 `true_value`。

**if(condition, true\_value, false\_value)**

如果 `condition` 为 true，则计算并返回 `true_value`，否则计算并返回 `false_value`。

## COALESCE

**coalesce(value1, value2\[, ...])**

返回参数列表中的第一个非 NULL `value`。和 `CASE` 表达式一样，仅在必要时计算参数。

## NULLIF

**nullif(value1, value2)**

如果 `value1` 等于 `value2`，则返回 NULL，否则返回 `value1`。

## TRY

**try(expression)**

计算表达式并通过返回 `NULL` 来处理特定类型的错误。

如果在遇到损坏或无效的数据时最好由查询生成 `NULL` 或默认值，而不是失败，则 `TRY` 函数可能是有用的。要指定默认值，可以将 `TRY` 函数与 `COALESCE` 函数配合使用。

`TRY` 处理以下错误：

- 被零除
- 无效的转换或函数参数
- 数值超出范围

### 示例

具有一些无效数据的源表：

``` sql
SELECT * FROM shipping;
```

```
origin_state | origin_zip | packages | total_cost
--------------+------------+----------+------------
California   |      94131 |       25 |        100
California   |      P332a |        5 |         72
California   |      94025 |        0 |        155
New Jersey   |      08544 |      225 |        490
(4 rows)
```

不带 `TRY` 的查询失败：

``` sql
SELECT CAST(origin_zip AS BIGINT) FROM shipping;
```

```
Query failed: Can not cast 'P332a' to BIGINT
```

带 `TRY` 的 `NULL` 值：

``` sql
SELECT TRY(CAST(origin_zip AS BIGINT)) FROM shipping;
```

```
origin_zip
------------
     94131
NULL
     94025
     08544
(4 rows)
```

不带 `TRY` 的查询失败：

``` sql
SELECT total_cost / packages AS per_package FROM shipping;
```

```
Query failed: Division by zero
```

带 `TRY` 和 `COALESCE` 的默认值：

``` sql
SELECT COALESCE(TRY(total_cost / packages), 0) AS per_package FROM shipping;
```

```
per_package
-------------
         4
        14
         0
        19
(4 rows)
```