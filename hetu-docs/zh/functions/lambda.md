
# Lambda表达式

Lambda表达式使用`->`表示：

    x -> x + 1
    (x, y) -> x + y
    x -> regexp_like(x, 'a+')
    x -> x[1] / x[2]
    x -> IF(x > 0, x, -x)
    x -> COALESCE(x, 0)
    x -> CAST(x AS JSON)
    x -> x + TRY(1 / 0)

大多数SQL表达式都可以用在lambda体中，但有一些例外：

- 不支持子查询：`x -> 2 + (SELECT 3)`
- 不支持聚合：`x -> max(y)`