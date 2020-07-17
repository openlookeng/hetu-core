
SHOW CREATE TABLE
=================

Synopsis
--------

``` sql
SHOW CREATE TABLE table_name
```

Description
-----------

Show the SQL statement that creates the specified table.

Examples
--------

Show the SQL that can be run to create the `orders` table:

    SHOW CREATE TABLE sf1.orders;

``` sql
Create Table
-----------------------------------------
CREATE TABLE tpch.sf1.orders (
orderkey bigint,
orderstatus varchar,
totalprice double,
orderdate varchar
)
WITH (
format = 'ORC',
partitioned_by = ARRAY['orderdate']
)
(1 row)
```

See Also
--------

[CREATE TABLE](./create-table.md)
