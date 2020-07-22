
# SHOW STATS

## 摘要

``` sql
SHOW STATS FOR table
SHOW STATS FOR ( SELECT * FROM table [ WHERE condition ] )
```

## 说明

返回命名表或（有限）查询结果的近似统计信息。

针对每列返回统计信息并返回一个摘要行。

| 列 | 说明 |
| ---------------------- | ------------------- |
| `column_name`             | 列的名称（对于摘要行为 `NULL`） |
| `data_size`               | 列中所有值的总大小（以字节为单位） |
| `distinct_values_count`   | 列中唯一值的数量 |
| `nulls_fractions`         | 列中为 `NULL` 的值部分 |
| `row_count`               | 行数（仅针对摘要行返回） |
| `low_value`               | 在该列中找到的最小值（仅适用于某些类型） |
| `high_value`              | 在该列中找到的最大值（仅适用于某些类型） |