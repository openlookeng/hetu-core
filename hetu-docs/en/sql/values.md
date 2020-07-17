
VALUES
======

Synopsis
--------

``` sql
VALUES row [, ...]
```

where `row` is a single expression or

``` sql
( column_expression [, ...] )
```

Description
-----------

Defines a literal inline table.

`VALUES` can be used anywhere a query can be used (e.g., the `FROM` clause of a [SELECT](./select.md), an [INSERT](./insert.md), or even at the top level). `VALUES` creates an anonymous table without column names, but the table and columns can be named using an `AS` clause with column aliases.

Examples
--------

Return a table with one column and three rows:

    VALUES 1, 2, 3

Return a table with two columns and three rows:

    VALUES
        (1, 'a'),
        (2, 'b'),
        (3, 'c')

Return table with column `id` and `name`:

    SELECT * FROM (
        VALUES
            (1, 'a'),
            (2, 'b'),
            (3, 'c')
    ) AS t (id, name)

Create a new table with column `id` and `name`:

    CREATE TABLE example AS
    SELECT * FROM (
        VALUES
            (1, 'a'),
            (2, 'b'),
            (3, 'c')
    ) AS t (id, name)

See Also
--------

[INSERT](./insert.md), [SELECT](./select.md)
