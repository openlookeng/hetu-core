
INSERT OVERWRITE
================

Synopsis
--------

``` sql
INSERT OVERWRITE [TABLE] table_name [ ( column [, ... ] ) ] query
```

Description
-----------

Insert overwrite basically do two things: 1) drop the data rows according to the dataset created by the query. 2) insert the new data created by query.

Insert overwrite can work on both partition and non-partition table, but the behaviors are different:

-   If the table is non-partition table, the existing data will be all deleted directly, and then insert the new data.
-   If the table is partitioned table, only the matched partition data which existing in the dataset result from query will be dropped and replaced with the new data.

If the list of column names is specified, they must exactly match the list of columns produced by the query. Each column in the table not present in the column list will be filled with a `null` value. Otherwise, if the list of columns is not specified, the columns produced by the query must exactly match the columns in the table being inserted into.

Examples
--------

Assume `orders` is not a partitioned table, and have 100 rows, then execute below insert overwrite statement:

    INSERT OVERWRITE orders VALUES (1, 'SUCCESS', '10.25', DATA '2020-01-01');

Then the `orders` table will only have 1 rows, that is the data specified in the `VALUE` clause.

Assume `users` has 3 columns: (`id`, `name`, `state`) and partitioned by `state`, and the existing data has follow rows:

---- ------ -------
  id   name   state
  1    John   CD
  2    Sam    CD
  3    Lucy   SZ
---- ------ -------

Then execute below insert overwrite statement:

    INSERT OVERWRITE orders VALUES (4, 'Newman', 'CD');

This will overwrite the data with partition value `state='CD'`, but wont impact the data `state='SZ'`. So the result will be

---- -------- -------
  id   name     state
  3    Lucy     SZ
  4    Newman   CD
---- -------- -------

Limitations
-----------

Right now only Hive Connector support insert overwrite.

See Also
--------

[VALUES](./values.md), [INSERT](./insert.md)
