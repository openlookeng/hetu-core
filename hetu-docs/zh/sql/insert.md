
# INSERT

## 摘要

``` sql
INSERT INTO table_name [ ( column [, ... ] ) ] query
```

## 说明

在表中插入新行。

如果指定了列名列表，则该列表必须与查询生成的列列表完全匹配。会使用一个 `null` 值填充表中未在列列表中显示的列。如果未指定列列表，则查询生成的列必须与要插入的表中的列完全匹配。

## 示例

将 `new_orders` 表中的其他行加载到 `orders` 表中：

    INSERT INTO orders
    SELECT * FROM new_orders;

在 `cities` 表中插入单行：

    INSERT INTO cities VALUES (1, 'San Francisco');

在 `cities` 表中插入多行：

    INSERT INTO cities VALUES (2, 'San Jose'), (3, 'Oakland');

使用指定的列列表在 `nation` 表中插入单行：

    INSERT INTO nation (nationkey, name, regionkey, comment)
    VALUES (26, 'POLAND', 3, 'no comment');

在不指定 `comment` 列的情况下插入单行，该列的值将为 `null`：

    INSERT INTO nation (nationkey, name, regionkey)
    VALUES (26, 'POLAND', 3);

## 另请参见

[VALUES](./values.md)、[INSERT OVERWRITE](./insert-overwrite.md)