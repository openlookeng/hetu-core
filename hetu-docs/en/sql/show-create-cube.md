
SHOW CREATE CUBE
=================

Synopsis
--------

``` sql
SHOW CREATE CUBE cube_name
```

Description
-----------

Show the SQL statement that creates the specified cube.

Examples
--------

Create a cube `orders_cube` on `orders` table as follows

    CREATE CUBE orders_cube ON orders WITH (AGGREGATIONS = (avg(totalprice), sum(totalprice), count(*)), 
    GROUP = (custKEY, ORDERkey), format= 'orc')

Use `SHOW CREATE CUBE` command to show the SQL statement that was used to create the cube `orders_cube`:

    SHOW CREATE CUBE orders_cube;

``` sql
CREATE CUBE orders_cube ON orders WITH (AGGREGATIONS = (avg(totalprice), sum(totalprice), count(*)), 
GROUP = (custKEY, ORDERkey), format= 'orc')
```
