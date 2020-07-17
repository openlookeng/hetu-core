
# 窗口函数

窗口函数对查询结果中的行执行计算。窗口函数在`HAVING`子句之后、`ORDER BY`子句之前运行。调用窗口函数需要使用特殊的语法（使用`OVER`子句来指定窗口）。一个窗口由三部分组成：

- 分区规范，它将输入行划分到不同的分区中。这与`GROUP BY`子句将行划分到不同的组中以用于聚合函数的方式类似。
- 排序规范，它确定窗口函数处理输入行的顺序。
- 窗口框架，它为给定的行指定要由函数处理的行滑动窗口。如果未指定框架，则默认使用`RANGE UNBOUNDED PRECEDING`，它与`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`相同。该框架包含从分区开头开始到当前行的最后一个对等点的所有行。

例如，以下查询按价格对每个售货员的订单进行排序：

    SELECT orderkey, clerk, totalprice,
           rank() OVER (PARTITION BY clerk
                        ORDER BY totalprice DESC) AS rnk
    FROM orders
    ORDER BY clerk, rnk

## 聚合函数

通过添加`OVER`子句，可以将所有`aggregate`用作窗口函数。会为当前行的窗口框架中的每个行计算聚合函数。

例如，以下查询为每个售货员生成按天滚动的订单价格总和：

    SELECT clerk, orderdate, orderkey, totalprice,
           sum(totalprice) OVER (PARTITION BY clerk
                                 ORDER BY orderdate) AS rolling_sum
    FROM orders
    ORDER BY clerk, orderdate, orderkey

## 排序函数

**cume\_dist()** -> bigint

返回一组值中某个值的累积分布。结果是窗口分区的窗口排序中的行前面或与之对等的行数量除以窗口分区中的总行数。因此，排序中的任何绑定值都将计算为相同的分布值。

**dense\_rank()** -> bigint

返回某个值在一组值中的排序。这与`rank`类似，只是绑定值不在序列中产生间隙。

**ntile(n)** -> bigint

将每个窗口分区的行分到`n`个桶中，范围为`1`至`n`（最大）。桶值最多相差`1`。如果分区中的行数未平均分成桶数，则从第一个桶开始每个桶分配一个剩余值。

例如，对于`6`行和`4`个桶，桶值如下：`1` `1` `2` `2` `3` `4`

**percent\_rank()** -> double

返回某个值在一组值中的百分比排名。结果是`(r - 1) / (n - 1)`，其中`r`是该行的`rank`，`n`是窗口分区中的总行数。

**rank() -> bigint**

返回某个值在一组值中的排序。排序是该行之前与该行不对等的行数加一。

因此，排序中的绑定值将在序列中产生间隙。会对每个窗口分区进行排序。

**row\_number()** -> bigint

根据窗口分区内行的排序，从1开始返回每行的唯一序列号。

## 值函数

**first\_value(x)** -> \[与输入相同]

返回窗口的第一个值。

**last\_value(x)** -> \[与输入相同]

返回窗口的最后一个值。

**nth\_value(x, offset)** -> \[与输入相同]

返回相对于窗口开头的指定偏移处的值。偏移从`1`开始。偏移可以是任何标量表达式。如果偏移为NULL或大于窗口中的值的数量，则返回NULL。如果偏移为零或负数，则产生错误。

**lead(x\[, offset \[, default\_value]])** -> \[与输入相同]

返回窗口中当前行之后`offset`行处的值。偏移从`0`（当前行）开始。偏移可以是任何标量表达式。默认`offset`为`1`。如果偏移为NULL或大于窗口，则返回`default_value`，如果未指定该值，则返回`null`。

**lag(x\[, offset \[, default\_value]])** -> \[与输入相同]

返回窗口中当前行之前`offset`行处的值。偏移从`0`（当前行）开始。偏移可以是任何标量表达式。默认`offset`为`1`。如果偏移为NULL或大于窗口，则返回`default_value`，如果未指定该值，则返回`null`。