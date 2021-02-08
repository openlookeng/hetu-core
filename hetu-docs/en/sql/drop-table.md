
DROP TABLE
==========

Synopsis
--------

``` sql
DROP TABLE  [ IF EXISTS ] table_name
```

Description
-----------

Drop an existing table.

The optional `IF EXISTS` clause causes the error to be suppressed if the table does not exist.

Examples
--------

Drop the table `orders_by_date`:

    DROP TABLE orders_by_date

Drop the table `orders_by_date` if it exists:

    DROP TABLE IF EXISTS orders_by_date

See Also
--------

[ALTER TABLE](./alter-table.md), [CREATE TABLE](./create-table.md)
