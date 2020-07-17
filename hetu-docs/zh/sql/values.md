
# VALUES

## 摘要

``` sql
VALUES row [, ...]
```

其中 `row` 是单个表达式或

``` sql
( column_expression [, ...] )
```

## 说明

定义字面量内联表。

可以在任何可以使用查询的地方（如 [SELECT](./select.md) 和 [INSERT](./insert.md) 语句的`FROM` 子句甚至是在顶级中）使用 `VALUES`。`VALUES` 创建一个不含列名的匿名表，但可以使用带列别名的子句 `AS` 对表和列进行命名。

## 示例

返回一个具有一列和三行的表：

    VALUES 1, 2, 3

返回一个具有两列和三行的表：

    VALUES
        (1, 'a'),
        (2, 'b'),
        (3, 'c')

返回一个具有列 `id` 和 `name` 的表：

    SELECT * FROM (
        VALUES
            (1, 'a'),
            (2, 'b'),
            (3, 'c')
    ) AS t (id, name)

创建一个具有列 `id` 和 `name` 的表：

    CREATE TABLE example AS
    SELECT * FROM (
        VALUES
            (1, 'a'),
            (2, 'b'),
            (3, 'c')
    ) AS t (id, name)

## 另请参见

[INSERT](./insert.md)、[SELECT](./select.md)