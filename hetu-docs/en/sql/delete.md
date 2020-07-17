
DELETE
======

Synopsis
--------

``` sql
DELETE FROM table_name [ WHERE condition ]
```

Description
-----------

Delete rows from a table. If the `WHERE` clause is specified, only the matching rows are deleted. Otherwise, all rows from the table are deleted.

Examples
--------

Delete all line items shipped by air:

    DELETE FROM lineitem WHERE shipmode = 'AIR';

Delete all line items for low priority orders:

    DELETE FROM lineitem
    WHERE orderkey IN (SELECT orderkey FROM orders WHERE priority = 'LOW');

Delete all orders:

    DELETE FROM orders;

Limitations
-----------

Some connectors have limited or no support for `DELETE`.

For example, to Hive connector, the behavior is different for transactional table and non-transactional table.

For transactional table, you can delete any row by where condition. However, for non-transactional table, DELETE is only supported if the WHERE clause matches entire partitions.

See connector documentation for more details.
