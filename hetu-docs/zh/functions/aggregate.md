
# 聚合函数

聚合函数对一组值进行操作以计算得出单个结果。

除了 `count`、`count_if`、`max_by`、`min_by` 和 `approx_distinct` 之外，所有这些聚合函数都忽略空值，并且对于没有输入行或所有值都是 NULL 的情况都返回 NULL。例如，`sum` 返回 NULL 而不是零，`avg` 在计数中不包括 NULL 值。可以使用 `coalesce` 函数将 NULL 转换为零。

某些聚合函数（如 `array_agg`）会根据输入值的顺序生成不同的结果。可以通过在聚合函数中写入一个 `order-by-clause` 来指定该顺序：

    array_agg(x ORDER BY y DESC)
    array_agg(x ORDER BY x, y, z)

## 一般聚合函数

**arbitrary(x)** -> \[与输入相同]

返回 `x` 的任意非 NULL 值（如果存在）。

**array\_agg(x**) -> array\<\[与输入相同]>

返回通过输入 `x` 元素创建的数组。

**avg(x)** -> double

返回所有输入值的平均值（算术平均值）。

**avg(时间间隔类型)** -> 时间间隔类型

返回所有输入值的平均间隔长度。

**bool\_and(boolean)** -> boolean

如果每个输入值都为 `TRUE`，则返回 `TRUE`，否则返回 `FALSE`。

**bool\_or(boolean)** -> boolean

如果任一输入值为 `TRUE`，则返回 `TRUE`，否则返回 `FALSE`。

**checksum(x)** -> varbinary

返回给定值的不区分顺序的校验和。

**count(\*)** -> bigint

返回输入行的数量。

**count(x)** -> bigint

返回非 NULL 输入值的数量。

**count\_if(x)** -> bigint

返回 `TRUE` 输入值的数量。该函数等价于 `count(CASE WHEN x THEN 1 END)`。

**every(boolean)** -> boolean

这是 `bool_and` 的别名。

**geometric\_mean(x)** -> double

返回所有输入值的几何平均值。

**max\_by(x, y)** -> \[与 x 相同]

返回与所有输入值中 `y` 的最大值相关联的 `x` 值。

**max\_by(x, y, n)** -> array\<\[与 x 相同]>

返回与 `y` 降序排列时 `y` 的所有输入值中前 `n` 个最大值相关联的 `n` 个 `x` 值。

**min\_by(x, y)** -> \[与 x 相同]

返回与所有输入值中 `y` 的最小值相关联的 `x` 值。

**min\_by(x, y, n)** -> array\<\[与 x 相同]>

返回与 `y` 升序排列时 `y` 的所有输入值中前 `n` 个最小值相关联的 `n` 个 `x` 值。

**max(x)** -> \[与输入相同]

返回所有输入值中的最大值。

**max(x, n)** -> array\<\[与 x 相同]>

返回 `x` 的所有输入值中的前 `n` 个最大值。

**min(x)** -> \[与输入相同]

返回所有输入值中的最小值。

**min(x, n)** -> array\<\[与 x 相同]>

返回 `x` 的所有输入值中的前 `n` 个最小值。

**sum(x)** -> \[与输入相同]

返回所有输入值的总和。

## 按位聚合函数

**bitwise\_and\_agg(x)** -> bigint

返回以二进制补码表示的所有输入值的按位与运算结果。

**bitwise\_or\_agg(x)** -> bigint

返回以二进制补码表示的所有输入值的按位或运算结果。

## 映射聚合函数

**histogram(x)** -> map(K,bigint)

返回一个映射，其中包含每个输入值出现的次数。

**map\_agg(key, value)** -> map(K,V)

返回通过输入 `key`/`value` 对创建的映射。

**map\_union(x(K,V))** -> map(K,V)

返回所有输入映射的并集。如果在多个输入映射中找到某个键，则生成的映射中该键的值来自任意输入映射。

**multimap\_agg(key, value)** -> map(K,array(V))

返回通过输入 `key`/`value` 对创建的多重映射。每个键可以关联多个值。

## 近似聚合函数

**approx\_distinct(x)** -> bigint

返回非重复输入值的近似数量。该函数提供 `count(DISTINCT x)` 的近似值。如果所有输入值都为 NULL，则返回零。

该函数应产生 2.3% 的标准误差，这是所有可能集合上的（近似正态）误差分布的标准差。该函数不保证任何特定输入集的误差上限。

**approx\_distinct(x, e)** -> bigint

返回非重复输入值的近似数量。该函数提供 `count(DISTINCT x)` 的近似值。如果所有输入值都为 NULL，则返回零。

该函数应产生大于 `e` 的标准误差，这是所有可能集合上的（近似正态）误差分布的标准差。该函数不保证任何特定输入集的误差上限。该函数的当前实现要求 `e` 处于 `[0.0040625, 0.26000]` 的范围之内。

**approx\_percentile(x, percentage)** -> \[与 x 相同]

返回在给定 `percentage` 时 `x` 的所有输入值的近似百分位数。`percentage` 值必须介于 0 和 1 之间，并且对于所有输入行是一个常量。

**approx\_percentile(x, percentages)** -> array\<\[与 x 相同]>

返回 `x` 的所有输入值在每个指定百分比处的近似百分位数。`percentages` 数组的每个元素必须介于 0 和 1 之间，并且该数组对于所有输入行必须是一个常量。

**approx\_percentile(x, w, percentage)** -> \[与 x 相同]

返回在百分比 `p` 处 `x` 的所有输入值（使用每项权重 `w`）的近似加权百分位数。权重必须是一个整数值，最小为 1。它实际上是百分位集中值 `x` 的重复计数。`p` 值必须介于 0 和 1 之间，并且对于所有输入行是一个常量。

**approx\_percentile(x, w, percentage, accuracy)** -> \[与 x 相同]

返回在百分比 `p` 处 `x` 的所有输入值（使用每项权重 `w`）的近似加权百分位数，最大排序误差为 `accuracy`。权重必须是一个整数值，最小为 1。它实际上是百分位集中值 `x` 的重复计数。`p` 值必须介于 0 和 1 之间，并且对于所有输入行是一个常量。`accuracy` 必须是一个大于 0 且小于 1 的值，并且对于所有输入行是一个常量。

**approx\_percentile(x, w, percentages)** -> array\<\[与 x 相同]>

返回在数组中指定的每个给定百分比处 `x` 的所有输入值（使用每项权重 `w`）的近似加权百分位数。权重必须是一个整数值，最小为 1。它实际上是百分位集中值 `x` 的重复计数。数组的每个元素必须介于 0 和 1 之间，并且该数组对于所有输入行必须是一个常量。

**approx\_set(x)** -> HyperLogLog

请参见 `hyperloglog`。

**merge(x)** -> HyperLogLog

请参见 `hyperloglog`。

**merge(qdigest(T))** -> qdigest(T)

请参见 `qdigest`。

**qdigest\_agg(x)** -> qdigest\<\[与 x 相同]>

请参见 `qdigest`。

**qdigest\_agg(x, w)** -> qdigest\<\[与 x 相同]>

请参见 `qdigest`。

**qdigest\_agg(x, w, accuracy)** -> qdigest\<\[与 x 相同]>

请参见 `qdigest`。

**numeric\_histogram(buckets, value, weight)** -> map\<double, double>

计算所有 `value`（每项权重为 `weight`）的近似直方图（存储桶的数量达 `buckets` 个）。算法大致基于：

```
Yael Ben-Haim and Elad Tom-Tov, "A streaming parallel decision tree algorithm",
J. Machine Learning Research 11 (2010), pp. 849--872.
```

`buckets` 必须是 `bigint`。`value` 和`weight` 必须是数字。

**numeric\_histogram(buckets, value)** -> map\<double, double>

计算所有 `value` 的近似直方图（存储桶的数量达 `buckets` 个）。该函数相当于 `numeric_histogram` 的变体，接受 `weight`，每项权重为 `1`。

## 统计聚合函数

**corr(y, x)** -> double

返回输入值的相关系数。

**covar\_pop(y, x)** -> double

返回输入值的总体协方差。

**covar\_samp(y, x)** -> double

返回输入值的样本协方差。

**kurtosis(x)** -> double

返回所有输入值的超值峰度。使用以下表达式的无偏估计：

```
kurtosis(x) = n(n+1)/((n-1)(n-2)(n-3))sum[(x_i-mean)^4]/stddev(x)^4-3(n-1)^2/((n-2)(n-3))
```

**regr\_intercept(y, x)**-> double

返回输入值的线性回归截距。`y` 为非独立值。`x` 为独立值。

**regr\_slope(y, x)** -> double

返回输入值的线性回归斜率。`y` 为非独立值。`x` 为独立值。

**skewness(x)** -> double

返回所有输入值的偏度。

**stddev(x)** -> double

这是 `stddev_samp` 的别名。

**stddev\_pop(x)**-> double

返回所有输入值的总体标准差。

**stddev\_samp(x)** -> double

返回所有输入值的样本标准差。

**variance(x)**-> double

这是 `var_samp` 的别名。

**var\_pop(x)**-> double

返回所有输入值的总体方差。

**var\_samp(x)**-> double

返回输入值的样本方差。

## lambda 聚合函数

**reduce\_agg(inputValue T, initialState S, inputFunction(S, T, S), combineFunction(S, S, S))** -> S

将所有输入值缩减为单个值。会为每个非 NULL 输入值调用 `inputFunction`。除了接受输入值之外，`inputFunction` 还接受当前状态（最初为 `initialState`）并返回新状态。会调用 `combineFunction` 将两个状态合并成一个新的状态。返回最终状态：

    SELECT id, reduce_agg(value, 0, (a, b) -> a + b, (a, b) -> a + b)
    FROM (
        VALUES
            (1, 3),
            (1, 4),
            (1, 5),
            (2, 6),
            (2, 7)
    ) AS t(id, value)
    GROUP BY id;
    -- (1, 12)
    -- (2, 13)
    
    SELECT id, reduce_agg(value, 1, (a, b) -> a * b, (a, b) -> a * b)
    FROM (
        VALUES
            (1, 3),
            (1, 4),
            (1, 5),
            (2, 6),
            (2, 7)
    ) AS t(id, value)
    GROUP BY id;
    -- (1, 60)
    -- (2, 42)

状态类型必须是布尔型、整型、浮点型或日期/时间/间隔型。