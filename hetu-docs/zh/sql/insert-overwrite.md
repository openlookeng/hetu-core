
# INSERT OVERWRITE

## 摘要

``` sql
INSERT OVERWRITE [TABLE] table_name [ ( column [, ... ] ) ] query
```

## 说明

insert overwrite 主要有两项功能：1\) 根据查询创建的数据集删除数据行；2\) 插入查询创建的新数据。

insert overwrite 可以对分区表和非分区表进行操作，但是行为不同：

- 对于非分区表，会直接删除所有的现有数据，然后插入新数据。
- 对于分区表，仅删除查询生成的数据集中存在的匹配分区数据，然后替换为新数据。

如果指定了列名列表，则该列表必须与查询生成的列列表完全匹配。会使用一个 `null` 值填充表中未在列列表中显示的列。如果未指定列列表，则查询生成的列必须与要插入的表中的列完全匹配。

## 示例

假设 `orders` 不是分区表，并且有 100 行，此时执行以下 insert overwrite 语句：

    INSERT OVERWRITE orders VALUES (1, 'SUCCESS', '10.25', DATA '2020-01-01');

则 `orders` 表将仅有 1 行，即 `VALUE` 子句中指定的数据。

假设 `users` 有 3 列（`id`、`name` 和 `state`）并按照 `state` 进行分区，并且现有数据有以下行：

-----------------

  id   name   state
  1    John   CD
  2    Sam    CD
  3    Lucy   SZ

-----------------


此时执行以下 insert overwrite 语句：

    INSERT OVERWRITE orders VALUES (4, 'Newman', 'CD');

这将使用分区值 `state='CD'` 覆盖数据，但不会影响数据 `state='SZ'`。因此结果将是

-------------------

  id   name     state
  3    Lucy     SZ
  4    Newman   CD

-------------------


## 限制

目前只有 Hive 连接器支持 insert overwrite。

## 另请参见

[VALUES](./values.md)、[INSERT](./insert.md)