
DROP CUBE
==========

Synopsis
--------

``` sql
DROP CUBE  [ IF EXISTS ] cube_name
```

Description
-----------

Drop an existing cube.

The optional `IF EXISTS` clause causes the error to be suppressed if the cube does not exist.

Examples
--------

Drop the cube `orders_cube`:

    DROP CUBE orders_cube

Drop the cube `orders_cube` if it exists:

    DROP CUBE IF EXISTS orders_cube

See Also
--------

[CREATE CUBE](./create-cube.md), [SHOW CUBES](./show-cubes.md), [INSERT INTO CUBE](./insert-cube.md)
