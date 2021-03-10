INSERT INTO CUBE
======

Synopsis
--------

``` sql
INSERT INTO CUBE cube_name [WHERE condition]
```

Description
-----------

Insert data into a star-tree cube. Predicate information is optional. If predicate provided, only data matching 
the given predicate are processed from the source table and inserted into the cube. Otherwise, entire 
data from the source table is processed and inserted into Cube.

Examples
--------

Insert data based on condition into the `orders_cube` cube:

    INSERT INTO CUBE orders_cube WHERE orderdate > date '1999-01-01';
    INSERT INTO CUBE order_all_cube;

See Also
--------

[INSERT OVERWRITE CUBE](./insert-overwrite-cube.md), [CREATE CUBE](./create-cube.md), [SHOW CUBES](./show-cubes.md), [DROP CUBE](./drop-cube.md)
