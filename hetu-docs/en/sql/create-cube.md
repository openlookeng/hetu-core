CREATE CUBE
============

Synopsis
--------

``` sql
CREATE CUBE [ IF NOT EXISTS ]
cube_name ON table_name WITH (
   AGGREGATIONS = ( expression [, ...] ), GROUP = ( column_name [, ...] )
   [, ( property_name = expression [, ...] ) ] 
)
```

Description
-----------

Create a new, empty star-tree cube with the specified group and aggregations. Use `insert-into-cube` to insert data.

The optional `IF NOT EXISTS` clause causes the error to be suppressed if the table already exists.

The optional `property_name` section can be used to set properties on the newly created cube. To list all available table properties, run the following query:

    SELECT * FROM system.metadata.table_properties

Examples
--------

Create a new cube `orders_cube` on `orders`:

    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), AVG(totalprice) ),
      GROUP = ( orderstatus, orderdate ),
      format = 'ORC'
    )

Create a new partitioned cube `orders_cube`:

    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), AVG(totalprice) ),
      GROUP = ( orderstatus, orderdate ),
      format = 'ORC',
      partitioned_by = ARRAY['orderdate']
    )

Limitations
-----------

- Supported aggregate functions:
      COUNT, COUNT DISTINCT, MIN, MAX, SUM, AVG
- Only one group is supported per Cube.  
- Different connector might support different data type, and different table/column properties.
- Can currently only create cubes in Hive connector, but the cubes can be created on a table from another connector. 

See Also
--------
[INSERT INTO CUBE](./insert-cube.md), [SHOW CUBES](./show-cubes.md), [DROP CUBE](./drop-cube.md)