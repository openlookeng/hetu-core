
UPDATE
======

Synopsis
--------

``` sql
UPDATE table_name SET column_name = expression[, column_name = expression, ... ] [ WHERE condition ]
```

Description
-----------

`UPDATE` changes the values of the specified columns in all rows that satisfy the condition. Only the columns to be modified need be mentioned in the SET clause; columns not explicitly modified retain their previous values.

Examples
--------

Update table `users`, change the name `Francisco` where the `id` equal to 1:

```sql
UPDATE users SET name = 'Francisco' WHERE id=1;
```

Limitations
-----------

-   Right now only Hive Connector and transactional ORC table support `UPDATE`.
-   The set expression does not support subquery.
-   Direct column reference is supported, but NOT expression with column reference.
-   `UPDATE` cannot be applied to view.
-   `UPDATE` dose not support implicitly data type conversion, please use `CAST` when value does not match target column's data type.
-   If the table is partitioned and/or bucketed, bucket\_column and partition column cannot be updated. i.e They cannot be target of SET expression.

See Also
--------

[INSERT](./insert.md), [INSERT OVERWRITE](./insert-overwrite.md)
