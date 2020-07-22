
# SELECT

## 摘要

``` sql
[ WITH with_query [, ...] ]
SELECT [ ALL | DISTINCT ] select_expression [, ...]
[ FROM from_item [, ...] ]
[ WHERE condition ]
[ GROUP BY [ ALL | DISTINCT ] grouping_element [, ...] ]
[ HAVING condition]
[ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] select ]
[ ORDER BY expression [ ASC | DESC ] [, ...] ]
[ OFFSET count [ ROW | ROWS ] ]
[ LIMIT { count | ALL } | FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } { ONLY | WITH TIES } ]
```

其中，`from_item`有如下两种形式：

``` sql
table_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
```

``` sql
from_item join_type from_item [ ON join_condition | USING ( join_column [, ...] ) ]
```

`join_type`是如下之一：

``` sql
[ INNER ] JOIN
LEFT [ OUTER ] JOIN
RIGHT [ OUTER ] JOIN
FULL [ OUTER ] JOIN
CROSS JOIN
```

`grouping_element`是如下之一：

``` sql
()
expression
GROUPING SETS ( ( column [, ...] ) [, ...] )
CUBE ( column [, ...] )
ROLLUP ( column [, ...] )
```

## 说明

从零个或多个表中检索行。

## WITH子句

`WITH`子句定义在查询中使用的命名关系。该子句可以实现扁平化嵌套查询或简化子查询。例如，以下两个查询是等价的：

    SELECT a, b
    FROM (
      SELECT a, MAX(b) AS b FROM t GROUP BY a
    ) AS x;
    
    WITH x AS (SELECT a, MAX(b) AS b FROM t GROUP BY a)
    SELECT a, b FROM x;

这也适用于多个子查询：

    WITH
      t1 AS (SELECT a, MAX(b) AS b FROM x GROUP BY a),
      t2 AS (SELECT a, AVG(d) AS d FROM y GROUP BY a)
    SELECT t1.*, t2.*
    FROM t1
    JOIN t2 ON t1.a = t2.a;

此外，`WITH`子句内的关系可以组成链式结构：

    WITH
      x AS (SELECT a FROM t),
      y AS (SELECT a AS b FROM x),
      z AS (SELECT b AS c FROM y)
    SELECT c FROM z;

**警告**

*目前，`WITH`子句的SQL将在使用命名关系的任何位置内联。这意味着，如果关系被多次使用，并且查询是非确定性的，那么每次的结果都可能不同。*

## SELECT子句

`SELECT`子句指定查询的输出。每个`select_expression`定义结果中要包含的一个或多个列。

``` sql
SELECT [ ALL | DISTINCT ] select_expression [, ...]
```

`ALL`和`DISTINCT`量词确定结果集中是否包含重复行。如果指定了参数`ALL`，则包含所有行。如果指定了参数`DISTINCT`，则结果集中只包含唯一的行。在这种情况下，每个输出列必须具有允许比较的类型。如果两个参数都没有指定，则行为默认为`ALL`。

**选择表达式**

每个`select_expression`必须采用下列其中一种形式：

``` sql
expression [ [ AS ] column_alias ]
```

``` sql
relation.*
```

``` sql
*
```

对于`expression [ [ AS ] column_alias ]`，定义了单个输出列。

对于`relation.*`，`relation`的所有列都包含在结果集中。

对于`*`，由查询定义的关系的所有列都包含在结果集中。

在结果集中，列的顺序与通过选择表达式指定列的顺序相同。如果选择表达式返回多个列，则这些列将按照源关系中的排序方式进行排序。

## GROUP BY子句

`GROUP BY`子句将`SELECT`语句的输出分组成包含匹配值的行。一个简单的`GROUP BY`子句可以包含任何由输入列组成的表达式，也可以是按位置选择输出列的序数（从1开始）。

以下查询是等价的。它们都通过`nationkey`输入列对输出进行分组，第一个查询使用输出列的顺序位置，第二个查询使用输入列名称：

    SELECT count(*), nationkey FROM customer GROUP BY 2;
    
    SELECT count(*), nationkey FROM customer GROUP BY nationkey;

`GROUP BY`子句可以按不在select语句的输出中显示的输入列名对输出进行分组。例如，以下查询使用输入列`mktsegment`来生成`customer`表的行数：

    SELECT count(*) FROM customer GROUP BY mktsegment;

```
_col0
-------
29968
30142
30189
29949
29752
(5 rows)
```

在`SELECT`语句中使用`GROUP BY`子句时，所有输出表达式必须是`GROUP BY`子句中出现的聚合函数或列。



**复杂分组操作**

openLooKeng还支持使用`GROUPING SETS`、`CUBE`和`ROLLUP`语法的复杂聚合。此语法允许用户在单个查询中执行需要对多组列进行聚合的分析。复杂分组操作不支持对由输入列组成的表达式进行分组。只支持列名或序号。

复杂分组操作通常与对简单`GROUP BY`表达式的`UNION ALL`操作等价，如下面的示例所示。但是，当聚合的数据源是非确定性的时，这种等价性不适用。

**GROUPING SETS**

GROUPING SETS允许用户指定要进行分组的多个列列表。不属于给定分组列子列表的列设置为`NULL`。如下所示：

    SELECT * FROM shipping;

```
origin_state | origin_zip | destination_state | destination_zip | package_weight
--------------+------------+-------------------+-----------------+----------------
California   |      94131 | New Jersey        |            8648 |             13
California   |      94131 | New Jersey        |            8540 |             42
New Jersey   |       7081 | Connecticut       |            6708 |            225
California   |      90210 | Connecticut       |            6927 |           1337
California   |      94131 | Colorado          |           80302 |              5
New York     |      10002 | New Jersey        |            8540 |              3
(6 rows)
```

这个示例查询演示了`GROUPING SETS`语义：

    SELECT origin_state, origin_zip, destination_state, sum(package_weight)
    FROM shipping
    GROUP BY GROUPING SETS (
        (origin_state),
        (origin_state, origin_zip),
        (destination_state));

```
origin_state | origin_zip | destination_state | _col0
--------------+------------+-------------------+-------
New Jersey   | NULL       | NULL              |   225
California   | NULL       | NULL              |  1397
New York     | NULL       | NULL              |     3
California   |      90210 | NULL              |  1337
California   |      94131 | NULL              |    60
New Jersey   |       7081 | NULL              |   225
New York     |      10002 | NULL              |     3
NULL         | NULL       | Colorado          |     5
NULL         | NULL       | New Jersey        |    58
NULL         | NULL       | Connecticut       |  1562
(10 rows)
```

上述查询在逻辑上可以等价于对多个`GROUP BY`查询的`UNION ALL`操作：

    SELECT origin_state, NULL, NULL, sum(package_weight)
    FROM shipping GROUP BY origin_state
    
    UNION ALL
    
    SELECT origin_state, origin_zip, NULL, sum(package_weight)
    FROM shipping GROUP BY origin_state, origin_zip
    
    UNION ALL
    
    SELECT NULL, NULL, destination_state, sum(package_weight)
    FROM shipping GROUP BY destination_state;

但是，使用复杂分组语法（`GROUPING SETS`、`CUBE`或`ROLLUP`）的查询将只从基础数据源读取一次，而使用`UNION ALL`的查询会读取基础数据三次。因此，当数据源不确定时，带有`UNION ALL`的查询可能产生不一致的结果。

**CUBE**

`CUBE`运算符生成所有可能的分组集（即幂集）的给定列集。例如，以下查询：

    SELECT origin_state, destination_state, sum(package_weight)
    FROM shipping
    GROUP BY CUBE (origin_state, destination_state);

等价于:

    SELECT origin_state, destination_state, sum(package_weight)
    FROM shipping
    GROUP BY GROUPING SETS (
        (origin_state, destination_state),
        (origin_state),
        (destination_state),
        ());

```
origin_state | destination_state | _col0
--------------+-------------------+-------
California   | New Jersey        |    55
California   | Colorado          |     5
New York     | New Jersey        |     3
New Jersey   | Connecticut       |   225
California   | Connecticut       |  1337
California   | NULL              |  1397
New York     | NULL              |     3
New Jersey   | NULL              |   225
NULL         | New Jersey        |    58
NULL         | Connecticut       |  1562
NULL         | Colorado          |     5
NULL         | NULL              |  1625
(12 rows)
```

**ROLLUP**

`ROLLUP`运算符为给定的列集生成所有可能的小记。例如，以下查询：

    SELECT origin_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY ROLLUP (origin_state, origin_zip);

```
origin_state | origin_zip | _col2
--------------+------------+-------
California   |      94131 |    60
California   |      90210 |  1337
New Jersey   |       7081 |   225
New York     |      10002 |     3
California   | NULL       |  1397
New York     | NULL       |     3
New Jersey   | NULL       |   225
NULL         | NULL       |  1625
(8 rows)
```

等价于:

    SELECT origin_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY GROUPING SETS ((origin_state, origin_zip), (origin_state), ());

**组合多个分组表达式**

同一查询中的多个分组表达式被解释为具有叉积语义。例如，以下查询：

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY
        GROUPING SETS ((origin_state, destination_state)),
        ROLLUP (origin_zip);

可以改写为：

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY
        GROUPING SETS ((origin_state, destination_state)),
        GROUPING SETS ((origin_zip), ());

逻辑上等价于：

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY GROUPING SETS (
        (origin_state, destination_state, origin_zip),
        (origin_state, destination_state));

```
origin_state | destination_state | origin_zip | _col3
--------------+-------------------+------------+-------
New York     | New Jersey        |      10002 |     3
California   | New Jersey        |      94131 |    55
New Jersey   | Connecticut       |       7081 |   225
California   | Connecticut       |      90210 |  1337
California   | Colorado          |      94131 |     5
New York     | New Jersey        | NULL       |     3
New Jersey   | Connecticut       | NULL       |   225
California   | Colorado          | NULL       |     5
California   | Connecticut       | NULL       |  1337
California   | New Jersey        | NULL       |    55
(10 rows)
```

`ALL`和`DISTINCT`量词确定重复分组集是否各自生成不同的输出行。这在多个复杂分组集组合在同一个查询中时尤其有用。例如，以下查询：

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY ALL
        CUBE (origin_state, destination_state),
        ROLLUP (origin_state, origin_zip);

等价于:

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY GROUPING SETS (
        (origin_state, destination_state, origin_zip),
        (origin_state, origin_zip),
        (origin_state, destination_state, origin_zip),
        (origin_state, origin_zip),
        (origin_state, destination_state),
        (origin_state),
        (origin_state, destination_state),
        (origin_state),
        (origin_state, destination_state),
        (origin_state),
        (destination_state),
        ());

但是，如果查询对`GROUP BY`使用`DISTINCT`量词：

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY DISTINCT
        CUBE (origin_state, destination_state),
        ROLLUP (origin_state, origin_zip);

只生成唯一的分组集合：

    SELECT origin_state, destination_state, origin_zip, sum(package_weight)
    FROM shipping
    GROUP BY GROUPING SETS (
        (origin_state, destination_state, origin_zip),
        (origin_state, origin_zip),
        (origin_state, destination_state),
        (origin_state),
        (destination_state),
        ());

默认的组量词是`ALL`。

**GROUPING操作**

`grouping(col1, ..., colN) -> bigint`

GROUPING操作返回转换为十进制的位集，指示分组中存在哪些列。它必须与`GROUPING SETS`、`ROLLUP`、`CUBE`或`GROUP BY`一起使用，并且其参数必须与相应的`GROUPING SETS`、`ROLLUP`、`CUBE`或`GROUP BY`子句中引用的列完全匹配。

要计算特定行的结果位集，将位分配给参数列，其中最右边的列是最低有效位。对于给定的分组，如果相应的列包含在分组中，则位设置为0，否则设置为1。例如，考虑以下查询：

    SELECT origin_state, origin_zip, destination_state, sum(package_weight),
           grouping(origin_state, origin_zip, destination_state)
    FROM shipping
    GROUP BY GROUPING SETS (
            (origin_state),
            (origin_state, origin_zip),
            (destination_state));

```
origin_state | origin_zip | destination_state | _col3 | _col4
--------------+------------+-------------------+-------+-------
California   | NULL       | NULL              |  1397 |     3
New Jersey   | NULL       | NULL              |   225 |     3
New York     | NULL       | NULL              |     3 |     3
California   |      94131 | NULL              |    60 |     1
New Jersey   |       7081 | NULL              |   225 |     1
California   |      90210 | NULL              |  1337 |     1
New York     |      10002 | NULL              |     3 |     1
NULL         | NULL       | New Jersey        |    58 |     6
NULL         | NULL       | Connecticut       |  1562 |     6
NULL         | NULL       | Colorado          |     5 |     6
(10 rows)
```

上述结果中的第一个分组只包括`origin_state`列，不包括`origin_zip`列和`destination_state`列。为该分组构造的位集是`011`，最高有效位表示`origin_state`。

## HAVING子句

`HAVING`子句与聚合函数和`GROUP BY`子句一起使用，以控制选择哪些组。`HAVING`子句排除不满足给定条件的组。分组和聚合计算完成后，`HAVING`对分组进行过滤。

以下示例查询`customer`表，选择余额大于指定金额的组。

    SELECT count(*), mktsegment, nationkey,
           CAST(sum(acctbal) AS bigint) AS totalbal
    FROM customer
    GROUP BY mktsegment, nationkey
    HAVING sum(acctbal) > 5700000
    ORDER BY totalbal DESC;

```
_col0 | mktsegment | nationkey | totalbal
-------+------------+-----------+----------
 1272 | AUTOMOBILE |        19 |  5856939
 1253 | FURNITURE  |        14 |  5794887
 1248 | FURNITURE  |         9 |  5784628
 1243 | FURNITURE  |        12 |  5757371
 1231 | HOUSEHOLD  |         3 |  5753216
 1251 | MACHINERY  |         2 |  5719140
 1247 | FURNITURE  |         8 |  5701952
(7 rows)
```

## UNION \| INTERSECT \| EXCEPT子句

`UNION`、`INTERSECT`和`EXCEPT`都是集合运算。这些子句用于将多个SELECT语句的结果组合成单个结果集：

``` sql
query UNION [ALL | DISTINCT] query
```

``` sql
query INTERSECT [DISTINCT] query
```

``` sql
query EXCEPT [DISTINCT] query
```

参数`ALL`或`DISTINCT`控制最终结果集中包括哪些行。如果指定了参数`ALL`，则包含所有行，即使这些行是相同的。如果指定了参数`DISTINCT`，则合并的结果集中只包含唯一的行。如果两个参数都没有指定，则行为默认为`DISTINCT`。`INTERSECT`或`EXCEPT`不支持`ALL`参数。

除非通过括号显式指定顺序，否则多个集合操作从左到右处理。另外，`INTERSECT`运算优先级高于`EXCEPT`和`UNION`。`A UNION B INTERSECT C EXCEPT D`意思和`A UNION (B INTERSECT C) EXCEPT D`一样。

**UNION**

`UNION`将第一个查询的结果集中的所有行与第二个查询的结果集中的所有行合并。下面是最简单的`UNION`子句之一的示例。该查询选择值`13`，并将这个结果集与选择值`42`的第二个查询组合在一起：

    SELECT 13
    UNION
    SELECT 42;

```
_col0
-------
   13
   42
(2 rows)
```

下面的查询演示了`UNION`和`UNION ALL`之间的区别。该查询选择值`13`，并将这个结果集与选择值`42`和`13`的第二个查询组合在一起：

    SELECT 13
    UNION
    SELECT * FROM (VALUES 42, 13);

```
_col0
-------
   13
   42
(2 rows)
```

    SELECT 13
    UNION ALL
    SELECT * FROM (VALUES 42, 13);

```
_col0
-------
   13
   42
   13
(2 rows)
```

**INTERSECT**

`INTERSECT`只返回同时存在于第一个和第二个查询的结果集中的行。下面是最简单的`INTERSECT`子句之一的示例。该查询选择值`13`和`42`，并将这个结果集与选择值`13`的第二个查询组合在一起：由于`42`只在第一个查询的结果集中，所以它不会包含在最终结果中。

    SELECT * FROM (VALUES 13, 42)
    INTERSECT
    SELECT 13;

```
_col0
-------
   13
(2 rows)
```

**EXCEPT**

`EXCEPT`返回在第一个查询结果集中但不在第二个查询结果集中的行。下面是最简单的`EXCEPT`子句之一的示例。该查询选择值`13`和`42`，并将这个结果集与选择值`13`的第二个查询组合在一起：由于`13`也在第二个查询的结果集中，所以它不会包含在最终结果中。

    SELECT * FROM (VALUES 13, 42)
    EXCEPT
    SELECT 13;

```
_col0
-------
  42
(2 rows)
```

## ORDER BY子句

`ORDER BY`子句用于按一个或多个输出表达式对结果集进行排序：

``` sql
ORDER BY expression [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...]
```

每个表达式可以由输出列组成，也可以是按位置选择输出列的序数（从1开始）。`ORDER BY`子句在任何`GROUP BY`或`HAVING`子句之后，任何`OFFSET`、`LIMIT`或`FETCH FIRST`子句之前执行。默认的NULL排序是`NULLS LAST`，无论排序方向如何都是如此。

## OFFSET子句

`OFFSET`子句用于从结果集中丢弃一些前导行：

``` sql
OFFSET count [ ROW | ROWS ]
```

如果`ORDER BY`子句存在，则`OFFSET`子句对已排序的结果集执行，并且该结果集在丢弃前导行之后仍保持排序：

    SELECT name FROM nation ORDER BY name OFFSET 22;

```
name
----------------
UNITED KINGDOM
UNITED STATES
VIETNAM
(3 rows)
```

否则将任意丢弃一些行。如果`OFFSET`子句中指定的计数等于或超过结果集的大小，则最终结果为空。

## LIMIT或FETCH FIRST子句

`LIMIT`或`FETCH FIRST`子句限制结果集中的行数。

``` sql
LIMIT { count | ALL }
```

``` sql
FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } { ONLY | WITH TIES }
```

以下示例查询一个大表，但`LIMIT`子句将输出限制为只有五行（因为查询缺少`ORDER BY`，所以将返回任意行）：

    SELECT orderdate FROM orders LIMIT 5;

```
orderdate
------------
1994-07-25
1993-11-12
1992-10-06
1994-01-04
1997-12-28
(5 rows)
```

`LIMIT ALL`等于省略了`LIMIT`子句。

`FETCH FIRST`子句支持`FIRST`或`NEXT`关键字，也支持`ROW`或`ROWS`关键字。这些关键字是等价的，关键字的选择对查询执行没有影响。

如果`FETCH FIRST`子句中没有指定计数，则默认为`1`：

    SELECT orderdate FROM orders FETCH FIRST ROW ONLY;

```
orderdate
------------
1994-02-12
(1 row)
```

如果存在`OFFSET`子句，则`LIMIT`或`FETCH FIRST`子句在`OFFSET`子句之后执行：

    SELECT * FROM (VALUES 5, 2, 4, 1, 3) t(x) ORDER BY x OFFSET 2 LIMIT 2;

```
x
---
3
4
(2 rows)
```

对于`FETCH FIRST`子句，参数`ONLY`或`WITH TIES`控制哪些行包括在结果集中。

如果指定了参数`ONLY`，结果集将限制为由计数确定的前导行的确切数目。

如果指定了参数`WITH TIES`，则要求存在`ORDER BY`子句。结果集由相同的前导行集和与之相同的对等组中的所有行组成。它们（“结”）是由`ORDER BY`子句中的顺序确定的。结果集被排序：

    SELECT name, regionkey FROM nation ORDER BY regionkey FETCH FIRST ROW WITH TIES;

```
name    | regionkey
------------+-----------
ETHIOPIA   |         0
MOROCCO    |         0
KENYA      |         0
ALGERIA    |         0
MOZAMBIQUE |         0
(5 rows)
```

## TABLESAMPLE

有多种采样方法：

`BERNOULLI`

每一行都以样本百分比的概率被选择到表样本中。当使用Bernoulli方法对表进行采样时，将扫描该表的所有物理块，并基于样本百分比与运行时计算的随机值之间的比较结果跳过某些行。

结果中包含某一行的概率与任何其他行无关。这不会减少从磁盘读取采样表所需的时间。如果进一步处理采样输出，则可能会影响总查询时间。

`SYSTEM`

这种采样方法将表划分为数据的逻辑段，并以此粒度对表进行采样。这种抽样方法要么从特定数据段中选择所有行，要么基于样本百分比与运行时计算的随机值之间的比较结果跳过该数据段。

在系统采样中选定的行将取决于所使用的连接器。例如，当与Hive一起使用时，取决于数据在HDFS上的分布。*这种方法不保证独立的采样概率。*

**注意**

这两种方法都不允许对返回的行数进行确定性限制。

示例：

    SELECT *
    FROM users TABLESAMPLE BERNOULLI (50);
    
    SELECT *
    FROM users TABLESAMPLE SYSTEM (75);

采样过程中使用JOIN：

    SELECT o.*, i.*
    FROM orders o TABLESAMPLE SYSTEM (10)
    JOIN lineitem i TABLESAMPLE BERNOULLI (40)
      ON o.orderkey = i.orderkey;

## UNNEST

`UNNEST`可用于将[数组](../language/types.md#array)或[映射](../language/types.md#map)展开为关系。数组被扩展为单列，而映射被扩展为两列（键值）。`UNNEST`也可以与多个参数一起使用，在这种情况下，它们会扩展为多列，行数为最高基数参数（其他列用NULL填充）。`UNNEST`也可以带`WITH ORDINALITY`子句，在这种情况下，一个额外的普通列被添加到末尾。`UNNEST`通常与`JOIN`一起使用，并且可以引JOIN左侧的关系中的列。

使用单列：

    SELECT student, score
    FROM tests
    CROSS JOIN UNNEST(scores) AS t (score);

使用多列：

    SELECT numbers, animals, n, a
    FROM (
      VALUES
        (ARRAY[2, 5], ARRAY['dog', 'cat', 'bird']),
        (ARRAY[7, 8, 9], ARRAY['cow', 'pig'])
    ) AS x (numbers, animals)
    CROSS JOIN UNNEST(numbers, animals) AS t (n, a);

```
numbers  |     animals      |  n   |  a
-----------+------------------+------+------
[2, 5]    | [dog, cat, bird] |    2 | dog
[2, 5]    | [dog, cat, bird] |    5 | cat
[2, 5]    | [dog, cat, bird] | NULL | bird
[7, 8, 9] | [cow, pig]       |    7 | cow
[7, 8, 9] | [cow, pig]       |    8 | pig
[7, 8, 9] | [cow, pig]       |    9 | NULL
(6 rows)
```

`WITH ORDINALITY`子句：

    SELECT numbers, n, a
    FROM (
      VALUES
        (ARRAY[2, 5]),
        (ARRAY[7, 8, 9])
    ) AS x (numbers)
    CROSS JOIN UNNEST(numbers) WITH ORDINALITY AS t (n, a);

```
numbers  | n | a
-----------+---+---
[2, 5]    | 2 | 1
[2, 5]    | 5 | 2
[7, 8, 9] | 7 | 1
[7, 8, 9] | 8 | 2
[7, 8, 9] | 9 | 3
(5 rows)
```

## JOIN

JOIN允许组合来自多个关系的数据。

### CROSS JOIN

CROSS JOIN返回两个关系的笛卡尔积（所有组合）。可以使用显式`CROSS JOIN`语法或通过在`FROM`子句中指定多个关系来指定交叉联接。

以下两个查询是等价的：

    SELECT *
    FROM nation
    CROSS JOIN region;
    
    SELECT *
    FROM nation, region;

`nation`表包含25行，`region`表包含5行，因此两个表之间的交叉联接将生成125行：

    SELECT n.name AS nation, r.name AS region
    FROM nation AS n
    CROSS JOIN region AS r
    ORDER BY 1, 2;

```
nation     |   region
----------------+-------------
ALGERIA        | AFRICA
ALGERIA        | AMERICA
ALGERIA        | ASIA
ALGERIA        | EUROPE
ALGERIA        | MIDDLE EAST
ARGENTINA      | AFRICA
ARGENTINA      | AMERICA
...
(125 rows)
```

### LATERAL

出现在`FROM`子句中的子查询前面可以加关键字`LATERAL`。这允许子查询引用前面`FROM`项提供的列。

`LATERAL`联接可以出现在`FROM`列表的顶层，或者括号括起来的联接树中的任何地方。在后一种情况下，对于联接在右侧的项目，联接也可以引用位于`JOIN`左侧的任何项目。

当`FROM`项包含`LATERAL`交叉引用时，计算过程如下：对于提供交叉引用列的`FROM`项的每一行，将使用该行集的列值对`LATERAL`项进行计算。所得行正常与它们从中计算出来的行连接在一起。对于列源表中的行集重复此过程。

`LATERAL`主要用于需要使用交叉引用的列来计算要连接的行的情形：

    SELECT name, x, y
    FROM nation
    CROSS JOIN LATERAL (SELECT name || ' :-' AS x)
    CROSS JOIN LATERAL (SELECT x || ')' AS y)

### 限定列名

当联接中的两个关系具有同名的列时，必须使用关系别名（如果关系具有别名）或使用关系名称对列引用进行限定：

    SELECT nation.name, region.name
    FROM nation
    CROSS JOIN region;
    
    SELECT n.name, r.name
    FROM nation AS n
    CROSS JOIN region AS r;
    
    SELECT n.name, r.name
    FROM nation n
    CROSS JOIN region r;

以下查询将失败，并返回错误`Column 'name' is ambiguous`：

    SELECT name
    FROM nation
    CROSS JOIN region;

## 子查询

子查询是由查询组成的表达式。当某个子查询引用该子查询外层的列时，该子查询称为相关子查询。逻辑上，会针对父查询中的每一行计算子查询。因此，在对子查询进行任何单个计算时，所引用的列将保持不变。

**说明**

*对相关子查询的支持有限。并不是每个标准表单都受支持。*

### EXISTS

`EXISTS`谓词确定子查询是否返回任何行：

    SELECT name
    FROM nation
    WHERE EXISTS (SELECT * FROM region WHERE region.regionkey = nation.regionkey)

### IN

`IN`谓词确定子查询产生的任何值是否等于提供的表达式。`IN`的结果遵循NULL的标准规则。子查询必须生成一列：

    SELECT name
    FROM nation
    WHERE regionkey IN (SELECT regionkey FROM region)

### 标量子查询

标量子查询是返回零行或一行的不相关子查询。子查询生成多行时即出错。如果子查询没有输出行，则返回值为`NULL`：

    SELECT name
    FROM nation
    WHERE regionkey = (SELECT max(regionkey) FROM region)

**说明**

*目前标量子查询只能返回单列。*