# Usage
Cubes can be managed using any of the supported clients, such as hetu-cli located under the `bin` directory in the installation.

## CREATE CUBE
### Synopsis

``` sql
CREATE CUBE [ IF NOT EXISTS ]
cube_name ON table_name WITH (
   AGGREGATIONS = ( expression [, ...] ),
   GROUP = ( column_name [, ...])
   [, FILTER = (expression)]
   [, ( property_name = expression [, ...] ) ] 
)
[WHERE predicate]
```
### Description
Create a new, empty Cube with the specified group and aggregations. Use `INSERT INTO CUBE (see below)` to insert into data.

The optional `IF NOT EXISTS` clause causes the error to be suppressed if the Cube already exists.
The optional `property_name` section can be used to set properties on the newly created Cube. 

To list all available table properties, run the following query:

    SELECT * FROM system.metadata.table_properties

**Note:** These properties are limited to the Connector which the Cube is being created for.

### Examples
Create a new Cube `orders_cube` on `orders`:

    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), AVG(totalprice) ),
      GROUP = ( orderstatus, orderdate ),
      format = 'ORC'
    )

Create a new partitioned Cube `orders_cube`:

    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), AVG(totalprice) ),
      GROUP = ( orderstatus, orderdate ),
      format = 'ORC',
      partitioned_by = ARRAY['orderdate']
    )

Create a new Cube `orders_cube` with some source data filter:

    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), COUNT DISTINCT(orderid) ),
      GROUP = ( orderstatus ),
      FILTER = (orderdate BETWEEN 2512450 AND 2512460)
    )

Create a new Cube `orders_cube` with some additional predicate on Cube columns:

    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), COUNT DISTINCT(orderid) ),
      GROUP = ( orderstatus ),
      FILTER = (orderdate BETWEEN 2512450 AND 2512460)
    ) WHERE orderstatus = 'PENDING';

This is same as following:
    
    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), COUNT DISTINCT(orderid) ),
      GROUP = ( orderstatus ),
      FILTER = (orderdate BETWEEN 2512450 AND 2512460)
    );
    INSERT INTO CUBE orders_cube WHERE orderstatus = 'PENDING';

The `FILTER` property can be used to filter out data from the source table while building the Cube. Cube is built on the data after 
applying the `orderdate BETWEEN 2512450 AND 2512460` predicate on the source table. The columns used in the filter predicate must not be part the Cube.

### Limitations
- Cubes can be created with only following aggregation functions.  
  In other words, Queries using the following functions can only be optimized using Cubes.
  **COUNT, COUNT DISTINCT, MIN, MAX, SUM, AVG**
- Different connector might support different data type, and different table/column properties.

## INSERT INTO CUBE

### Synopsis
``` sql
INSERT INTO CUBE cube_name [WHERE condition]
```

### Description
`CREATE CUBE` statement creates Cube without any data. To insert data into Cube, use `INSERT INTO CUBE` SQL.
The `WHERE` clause is optional. If predicate is provided, only data matching the given predicate are processed from the source table and inserted into the Cube. 
Otherwise, entire data from the source table is processed and inserted into Cube.

### Examples
Insert data into the `orders_cube` Cube:

    INSERT INTO CUBE orders_cube WHERE orderdate > date '1999-01-01';
    INSERT INTO CUBE order_all_cube;

### Limitations
1. Subsequent inserts to the same Cube need to use same set of columns

```sql
   CREATE CUBE orders_cube ON orders WITH (AGGREGATIONS = (count(*)), GROUP = (orderdate));
   
   INSERT INTO CUBE orders_cube WHERE orderdate BETWEEN date '1999-01-01' AND date '1999-01-05';
   
   -- This statement would fail because its possible the Cube already contain rows matching the given predicate.
   INSERT INTO CUBE orders_cube WHERE location = 'Canada';
```
**Note:** This means that columns used in the first insert must be used in every insert predicate following the first to avoid inserting duplicate data.

## INSERT OVERWRITE CUBE

### Synopsis
``` sql
INSERT OVERWRITE CUBE cube_name [WHERE condition]
```

### Description
Similar to INSERT INTO CUBE statement but with this statement the existing data is overwritten. Predicates
are optional.

### Examples
Insert data based on condition into the `orders_cube` Cube:

    INSERT OVERWRITE CUBE orders_cube WHERE orderdate > date '1999-01-01';
    INSERT OVERWRITE CUBE orders_cube;

## SHOW CUBES

### Synopsis
```sql
SHOW CUBES [ FOR table_name ];
```

### Description
`SHOW CUBES` lists all Cubes. Adding the optional `table_name` lists only the Cubes for that table.

### Examples

Show all Cubes:
```sql
    SHOW CUBES;
```

Show Cubes for `orders` table:

```sql
    SHOW CUBES FOR orders;
```

## DROP CUBE

### Synopsis

``` sql
DROP CUBE  [ IF EXISTS ] cube_name
```

### Description
Drop an existing Cube.

The optional `IF EXISTS` clause causes the error to be suppressed if the Cube does not exist.

### Examples

Drop the Cube `orders_cube`:

    DROP CUBE orders_cube

Drop the Cube `orders_cube` if it exists:

    DROP CUBE IF EXISTS orders_cube


