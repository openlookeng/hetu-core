
SHOW CUBES
==========

Synopsis
--------

``` sql
SHOW CUBES [ FOR table_name ];
```

Description
-----------

`SHOW CUBES` lists all cubes. Adding the optional `table_name` lists only the cubes for that table.

Examples
--------

Show all cubes:

```sql
    SHOW CUBES;
```

Show cubes for `orders` table:

```sql
    SHOW CUBES FOR orders;
```

See Also
--------

[CREATE CUBE](./create-cube.md), [DROP CUBE](./drop-cube.md), [INSERT INTO CUBE](./insert-cube.md)
