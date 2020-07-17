
# 比较函数和运算符

## 比较运算符

| 运算符 | 说明                             |
| :----- | :------------------------------- |
| `<`    | 小于                             |
| `>`    | 大于                             |
| `<=`   | 小于等于                         |
| `>=`   | 大于等于                         |
| `=`    | 等于                             |
| `<>`   | 不等于                           |
| `!=`   | 不等于（该语法不标准，但很常见） |

## 范围运算符：BETWEEN

`BETWEEN` 运算符测试某个值是否处于指定的范围之内。其语法为 `value BETWEEN min AND max`：

    SELECT 3 BETWEEN 2 AND 6;

上面显示的语句等效于以下语句：

    SELECT 3 >= 2 AND 3 <= 6;

要测试某个值是否未处于指定的范围之内，应使用 `NOT BETWEEN`：

    SELECT 3 NOT BETWEEN 2 AND 6;

上面显示的语句等效于以下语句：

    SELECT 3 < 2 OR 3 > 6;

使用向上面等效的表达式应用的标准 `NULL` 计算规则对 `BETWEEN` 或 `NOT BETWEEN` 语句中的 `NULL` 进行计算：

    SELECT NULL BETWEEN 2 AND 4; -- null
    
    SELECT 2 BETWEEN NULL AND 6; -- null
    
    SELECT 2 BETWEEN 1 AND NULL; -- false
    
    SELECT 8 BETWEEN NULL AND 6; -- false

`BETWEEN` 和 `NOT BETWEEN` 运算符也可用于计算任何可排序的类型。例如 `VARCHAR` 类型：

    SELECT 'Paul' BETWEEN 'John' AND 'Ringo'; -- true

请注意，`BETWEEN` 和 `NOT BETWEEN` 的 value、min 和 max 参数必须具有相同的类型。例如，如果您向 openLooKeng 询问 John 是否处于 2.3 和 35.2 之间，它会产生一个错误。

## IS NULL 和 IS NOT NULL

`IS NULL` 和 `IS NOT NULL` 运算符测试某个值是否为 NULL（未定义）。这两个运算符都对所有数据类型起作用。

如果对 `IS NULL` 使用 `NULL`，则计算结果为 true：

    select NULL IS NULL; -- true

但使用其他任何常量的计算结果都不是 true：

    SELECT 3.0 IS NULL; -- false

## IS DISTINCT FROM 和 IS NOT DISTINCT FROM

在 SQL 中，`NULL` 值表示一个未知的值，所以任何涉及 `NULL` 的比较都会生成 `NULL`。`IS DISTINCT FROM` 和 `IS NOT DISTINCT FROM` 运算符将 `NULL` 视为一个已知的值，并且都保证即使在存在 `NULL` 输入的情况下结果也为 true 或 false：

    SELECT NULL IS DISTINCT FROM NULL; -- false
    
    SELECT NULL IS NOT DISTINCT FROM NULL; -- true

在上面的示例中，`NULL` 值被视为与 `NULL` 相同。在比较可能包含 `NULL` 的值时，使用这些运算符来保证结果为 `TRUE` 或 `FALSE`。

下面的真值表展示了如何处理 `IS DISTINCT FROM` 和 `IS NOT DISTINCT FROM` 中的 `NULL`：

| a      | b      | a = b   | a <> b  | a DISTINCT b | a NOT DISTINCT b |
| :----- | :----- | :------ | :------ | :----------- | :--------------- |
| `1`    | `1`    | `TRUE`  | `FALSE` | `FALSE`      | `TRUE`           |
| `1`    | `2`    | `FALSE` | `TRUE`  | `TRUE`       | `FALSE`          |
| `1`    | `NULL` | `NULL`  | `NULL`  | `TRUE`       | `FALSE`          |
| `NULL` | `NULL` | `NULL`  | `NULL`  | `FALSE`      | `TRUE`           |

## GREATEST 和 LEAST

这两个函数不是 SQL 标准中的函数，而是常见的扩展函数。与 openLooKeng 中的大多数其他函数一样，如果任一参数为 NULL，这两个函数会返回 NULL。请注意，在其他一些数据库（如 PostgreSQL）中，只有当所有参数都是 NULL 时它们才返回 NULL。

支持以下类型：`DOUBLE`、`BIGINT`、`VARCHAR`、`TIMESTAMP`、`TIMESTAMP WITH TIME ZONE`、`DATE`

**greatest(value1, value2, ..., valueN)** -> \[与输入相同]

返回所提供的值中的最大值。

**least(value1, value2, ..., valueN)** -> \[与输入相同]

返回所提供的值中的最小值。

## 量化比较谓词：ALL、ANY 和 SOME

可以按照以下方法将 `ALL`、`ANY` 和 `SOME` 量词与比较运算符配合使用：

```
expression operator quantifier ( subquery )
```

例如：

    SELECT 'hello' = ANY (VALUES 'hello', 'world'); -- true
    
    SELECT 21 < ALL (VALUES 19, 20, 21); -- false
    
    SELECT 42 >= SOME (SELECT 41 UNION ALL SELECT 42 UNION ALL SELECT 43); -- true

以下是一些量词和比较运算符组合的含义：

| 表达式           | 含义                                                         |
| :--------------- | :----------------------------------------------------------- |
| `A = ALL (...)`  | 当 `A` 等于所有值时计算结果为 `true`。                       |
| `A <> ALL (...)` | 当 `A` 不匹配任何值时计算结果为 `true`。                     |
| `A < ALL (...)`  | 当 `A` 小于最小值时计算结果为 `true`。                       |
| `A = ANY (...)`  | 当 `A` 等于任一值时计算结果为 `true`。该形式等效于 `A IN (...)`。 |
| `A <> ANY (...)` | 当 `A` 不匹配一个或多个值时计算结果为 `true`。               |
| `A < ANY (...)`  | 当 `A` 小于最大值时计算结果为 `true`。                       |

