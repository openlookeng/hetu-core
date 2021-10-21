
SHOW STATS
==========

Synopsis
--------

``` sql
SHOW STATS FOR table
SHOW STATS FOR ( SELECT * FROM table [ WHERE condition ] )
```

Description
-----------

Returns approximated statistics for the named table or for the results of a (limited) query.

Statistics are returned for each column, along with a summary row.

| Column | Description |
| ------------------------- | --------------------------------------- |
| `column_name`             | The name of the column (`NULL` for the summary row). |
| `data_size`               | The total size in bytes of all of the values in the column. |
| `distinct_values_count`   | The number of distinct values in the column. |
| `nulls_fractions`         | The portion of the values in the column that are `NULL`. |
| `row_count`               | The number of rows (only returned for the summary row). |
| `low_value`               | The lowest value found in this column (only for some types). |
| `high_value`              | The highest value found in this column (only for some types). |