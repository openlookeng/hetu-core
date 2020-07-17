
# 数组函数和运算符

## 下标运算符： \[]

`[]` 运算符用于访问数组中的一个元素，其索引从 1 开始：

    SELECT my_array[1] AS first_element

## 连接运算符： \|\|

`||` 运算符用于将数组与一个相同类型的数组或元素进行连接：

    SELECT ARRAY [1] || ARRAY [2]; -- [1, 2]
    SELECT ARRAY [1] || 2; -- [1, 2]
    SELECT 2 || ARRAY [1]; -- [2, 1]

## 数组函数

**array\_distinct(x)** -> array

删除数组 `x` 中的重复值。

**array\_intersect(x, y)**-> array

返回 `x` 与 `y` 的交集中的元素构成的数组，不含重复元素。

**array\_union(x, y)** -> array

返回 `x` 与 `y` 的并集中的元素构成的数组，不含重复元素。

**array\_except(x, y)** -> array

返回位于 `x` 但不位于 `y` 中的元素构成的数组，不含重复元素。

**array\_join(x, delimiter, null\_replacement)** -> varchar

使用分隔符和一个用于替换 NULL 的可选字符串连接给定数组的元素。

**array\_max(x)** -> x

返回输入数组中的最大值。

**array\_min(x)** -> x

返回输入数组中的最小值。

**array\_position(x, element)** -> bigint

返回数组 `x` 中 `element` 第一次出现的位置（如果没有找到，则返回 0）。

**array\_remove(x, element)** -> array

删除数组 `x` 中所有等于 `element` 的元素。

**array\_sort(x)** -> array

对数组 `x` 进行排序并返回该数组。`x` 的元素必须是可排序的。NULL 元素将被放置在返回的数组的末尾。

**array\_sort(array(T), function(T,T,int)) ** -\> array(T)

基于给定的比较函数 `function` 对 `array` 进行排序并将其返回。比较函数将接受两个可以为 NULL 的参数来表示 `array` 中两个可以为 NULL 的元素。当第一个可以为 NULL 的元素小于、等于或大于第二个可以为 NULL 的元素时，该函数返回 -1、0 或 1。如果比较函数返回其他值（包括 `NULL`），查询将失败并产生一个错误：

    SELECT array_sort(ARRAY [3, 2, 5, 1, 2], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))); -- [5, 3, 2, 2, 1]
    SELECT array_sort(ARRAY ['bc', 'ab', 'dc'], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))); -- ['dc', 'bc', 'ab']
    SELECT array_sort(ARRAY [3, 2, null, 5, null, 1, 2], -- sort null first with descending order
                      (x, y) -> CASE WHEN x IS NULL THEN -1
                                     WHEN y IS NULL THEN 1
                                     WHEN x < y THEN 1
                                     WHEN x = y THEN 0
                                     ELSE -1 END); -- [null, null, 5, 3, 2, 2, 1]
    SELECT array_sort(ARRAY [3, 2, null, 5, null, 1, 2], -- sort null last with descending order
                      (x, y) -> CASE WHEN x IS NULL THEN 1
                                     WHEN y IS NULL THEN -1
                                     WHEN x < y THEN 1
                                     WHEN x = y THEN 0
                                     ELSE -1 END); -- [5, 3, 2, 2, 1, null, null]
    SELECT array_sort(ARRAY ['a', 'abcd', 'abc'], -- sort by string length
                      (x, y) -> IF(length(x) < length(y),
                                   -1,
                                   IF(length(x) = length(y), 0, 1))); -- ['a', 'abc', 'abcd']
    SELECT array_sort(ARRAY [ARRAY[2, 3, 1], ARRAY[4, 2, 1, 4], ARRAY[1, 2]], -- sort by array length
                      (x, y) -> IF(cardinality(x) < cardinality(y),
                                   -1,
                                   IF(cardinality(x) = cardinality(y), 0, 1))); -- [[1, 2], [2, 3, 1], [4, 2, 1, 4]]

**arrays\_overlap(x, y)** -> boolean

测试数组 `x` 和 `y` 是否有任何共同的非 NULL 元素。如果没有共同的非 NULL 元素，但任一数组包含 NULL，则返回 NULL。

**cardinality(x)** -> bigint

返回数组 `x` 的基数（大小）。

**concat(array1, array2, ..., arrayN)**  -> array

连接数组 `array1`、`array2`、`...` `arrayN`。该函数提供与 SQL 标准连接运算符 (`||`) 相同的功能。

**combinations(array(T), n) -> array(array(T))**

返回输入数组的 n 元素子组。如果输入数组没有重复项，则 `combinations` 返回 n 元素子集：

    SELECT combinations(ARRAY['foo', 'bar', 'baz'], 2); -- [['foo', 'bar'], ['foo', 'baz'], ['bar', 'baz']]
    SELECT combinations(ARRAY[1, 2, 3], 2); -- [[1, 2], [1, 3], [2, 3]]
    SELECT combinations(ARRAY[1, 2, 2], 2); -- [[1, 2], [1, 2], [2, 2]]

子组的顺序是确定的，但未经指定。子组中元素的顺序是确定的，但未经指定。`n` 不得大于 5，生成的子组的总大小必须小于 100000。

**contains(x, element) -> boolean**

如果数组 `x` 包含 `element`，则返回 true。

**element\_at(array(E), index)** -> E

返回 `array` 在给定 `index` 处的元素。如果 `index` > 0，则该函数提供与 SQL 标准下标运算符 (`[]`) 相同的功能。如果 `index` \< 0，则 `element_at` 按照从最后一个到第一个的顺序访问元素。

**filter(array(T), function(T,boolean))** -> array(T)

通过 `function` 针对其返回 true 的 `array` 的元素构造一个数组。

    SELECT filter(ARRAY [], x -> true); -- []
    SELECT filter(ARRAY [5, -6, NULL, 7], x -> x > 0); -- [5, 7]
    SELECT filter(ARRAY [5, NULL, 7, NULL], x -> x IS NOT NULL); -- [5, 7]

**flatten(x)** -> array

通过连接包含的数组将 `array(array(T))` 扁平化为 `array(T)`。

**ngrams(array(T), n)** -> array(array(T))

返回 `array` 的 `n`-gram（包含 `n` 个相邻元素的子序列）。结果中 `n`-gram 的顺序未经指定。

    SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 2); -- [['foo', 'bar'], ['bar', 'baz'], ['baz', 'foo']]
    SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 3); -- [['foo', 'bar', 'baz'], ['bar', 'baz', 'foo']]
    SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 4); -- [['foo', 'bar', 'baz', 'foo']]
    SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 5); -- [['foo', 'bar', 'baz', 'foo']]
    SELECT ngrams(ARRAY[1, 2, 3, 4], 2); -- [[1, 2], [2, 3], [3, 4]]

**reduce(array(T), initialState S, inputFunction(S,T,S), outputFunction(S,R))** -> R

返回从 `array` 简化得到的单个值。会按顺序为 `array` 中的每个元素调用 `inputFunction`。除了接受元素之外，`inputFunction` 还接受当前状态（最初为 `initialState`）并返回新状态。会调用 `outputFunction` 以将最终状态转换为结果值。该函数可能是恒等函数 (`i -> i`)：

    SELECT reduce(ARRAY [], 0, (s, x) -> s + x, s -> s); -- 0
    SELECT reduce(ARRAY [5, 20, 50], 0, (s, x) -> s + x, s -> s); -- 75
    SELECT reduce(ARRAY [5, 20, NULL, 50], 0, (s, x) -> s + x, s -> s); -- NULL
    SELECT reduce(ARRAY [5, 20, NULL, 50], 0, (s, x) -> s + COALESCE(x, 0), s -> s); -- 75
    SELECT reduce(ARRAY [5, 20, NULL, 50], 0, (s, x) -> IF(x IS NULL, s, s + x), s -> s); -- 75
    SELECT reduce(ARRAY [2147483647, 1], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); -- 2147483648
    SELECT reduce(ARRAY [5, 6, 10, 20], -- calculates arithmetic average: 10.25
                  CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)),
                  (s, x) -> CAST(ROW(x + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
                  s -> IF(s.count = 0, NULL, s.sum / s.count));

**repeat(element, count)** -> array

将 `element` 重复 `count` 次。

**reverse(x)** -> array

返回一个数组，该数组中元素的顺序与数组 `x` 相反。

**sequence(start, stop)** -> array(bigint)

生成一个从 `start` 到 `stop` 的整数序列，如果 `start` 小于等于 `stop`，则以 `1` 为单位递增，否则以 `-1` 为单位递增。

**sequence(start, stop, step)** -> array(bigint)

生成一个从 `start` 到 `stop` 的整数序列，以 `step` 为单位递增。

**sequence(start, stop)** -> array(date)

生成一个从 `start` 日期到 `stop` 日期的日期序列，如果 `start` 日期小于等于 `stop` 日期，则以 `1` 天为单位递增，否则以 `-1` 天为单位递增。

**sequence(start, stop, step)** -> array(date)

生成一个从 `start` 到 `stop` 的序列，以 `step` 为单位递增。`step` 的类型可以是 `INTERVAL DAY TO SECOND` 或 `INTERVAL YEAR TO MONTH`。

**sequence(start, stop, step)** -> array(timestamp)

生成一个从 `start` 到 `stop`的时间戳序列，以 `step` 为单位递增。`step` 的类型可以是 `INTERVAL DAY TO SECOND` 或 `INTERVAL YEAR TO MONTH`。

**shuffle(x)** -> array

生成给定数组 `x` 的随机排列。

**slice(x, start, length)** -> array

从索引 `start` 开始（如果 `start` 为负数，则从末尾开始）生成数组 `x` 的子集，其长度为 `length`。

**transform(array(T), function(T,U))** -> array(U)

返回一个数组，该数组是对 `array` 的每个元素应用 `function` 的结果：

    SELECT transform(ARRAY [], x -> x + 1); -- []
    SELECT transform(ARRAY [5, 6], x -> x + 1); -- [6, 7]
    SELECT transform(ARRAY [5, NULL, 6], x -> COALESCE(x, 0) + 1); -- [6, 1, 7]
    SELECT transform(ARRAY ['x', 'abc', 'z'], x -> x || '0'); -- ['x0', 'abc0', 'z0']
    SELECT transform(ARRAY [ARRAY [1, NULL, 2], ARRAY[3, NULL]], a -> filter(a, x -> x IS NOT NULL)); -- [[1, 2], [3]]

**zip(array1, array2\[, ...])** -> array(row)

将给定的数组按元素合并到单个行数组中。第 N 个参数的第 M 个元素将是第 M 个输出元素的第 N 个字段。如果参数的长度不一致，则使用 `NULL` 填充缺少的值：

    SELECT zip(ARRAY[1, 2], ARRAY['1b', null, '3b']); -- [ROW(1, '1b'), ROW(2, null), ROW(null, '3b')]

**zip\_with(array(T), array(U), function(T,U,R))** -> array(R)

使用 `function` 将两个给定的数组按元素合并到单个数组中。如果一个数组较短，在应用 `function` 之前在其末尾添加 NULL 以匹配较长数组的长度：

    SELECT zip_with(ARRAY[1, 3, 5], ARRAY['a', 'b', 'c'], (x, y) -> (y, x)); -- [ROW('a', 1), ROW('b', 3), ROW('c', 5)]
    SELECT zip_with(ARRAY[1, 2], ARRAY[3, 4], (x, y) -> x + y); -- [4, 6]
    SELECT zip_with(ARRAY['a', 'b', 'c'], ARRAY['d', 'e', 'f'], (x, y) -> concat(x, y)); -- ['ad', 'be', 'cf']
    SELECT zip_with(ARRAY['a'], ARRAY['d', null, 'f'], (x, y) -> coalesce(x, y)); -- ['a', null, 'f']