
CREATE TABLE
============

Synopsis
--------

``` sql
CREATE TABLE [ IF NOT EXISTS ]
table_name (
  { column_name data_type [ COMMENT comment ] [ WITH ( property_name = expression [, ...] ) ]
  | LIKE existing_table_name [ { INCLUDING | EXCLUDING } PROPERTIES ] }
  [, ...]
)
[ COMMENT table_comment ]
[ WITH ( property_name = expression [, ...] ) ]
```

Description
-----------

Create a new, empty table with the specified columns. Use `create-table-as` to create a table with data.

The optional `IF NOT EXISTS` clause causes the error to be suppressed if the table already exists.

The optional `WITH` clause can be used to set properties on the newly created table or on single columns. To list all available table properties, run the following query:

    SELECT * FROM system.metadata.table_properties

For example, to hive connector, below are some of available and frequently used table properties:

| Property Name    | data type      | Description                                                  | Default |
| ---------------- | -------------- | ------------------------------------------------------------ | ------- |
| `format`         | varchar        | Hive storage format for the table. Possible values: [ORC, PARQUET, AVRO, RCBINARY, RCTEXT, SEQUENCEFILE, JSON, TEXTFILE, CSV] | ORC     |
| `bucket_count`   | integer        | Number of buckets                                            |         |
| `bucketed_by`    | array(varchar) | Bucketing columns                                            |         |
| `sorted_by`      | array(varchar) | Bucket sorting columns                                       |         |
| `external`       | boolean        | Is the table an external table                               | `false` |
| `location`       | varchar        | File system location URI for the table location value must be provided if `external`=`true` |         |
| `partitioned_by` | array(varchar) | Partition columns                                            |         |
| `transactional`  | boolean        | Is transactional property enabled There is a limitation that only ORC Storage format support creating an transactional table | `false` |

To list all available column properties, run the following query:

    SELECT * FROM system.metadata.column_properties

The `LIKE` clause can be used to include all the column definitions from an existing table in the new table. Multiple `LIKE` clauses may be specified, which allows copying the columns from multiple tables.

If `INCLUDING PROPERTIES` is specified, all of the table properties are copied to the new table. If the `WITH` clause specifies the same property name as one of the copied properties, the value from the `WITH`
clause will be used. The default behavior is `EXCLUDING PROPERTIES`. The `INCLUDING PROPERTIES` option maybe specified for at most one table.

Examples
--------

Create a new table `orders`:

    CREATE TABLE orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double,
      orderdate date
    )
    WITH (format = 'ORC')

Create a new transactional table `orders`:

    CREATE TABLE orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double,
      orderdate date
    )
    WITH (format = 'ORC',
    transactional=true)

Create an external table `orders`:

    CREATE TABLE orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double,
      orderdate date
    )
    WITH (format = 'ORC',
    external=true,
    location='hdfs://hdcluster/tmp/externaltbl')

Create the table `orders` if it does not already exist, adding a table comment and a column comment:

    CREATE TABLE IF NOT EXISTS orders (
      orderkey bigint,
      orderstatus varchar,
      totalprice double COMMENT 'Price in cents.',
      orderdate date
    )
    COMMENT 'A table to keep track of orders.'

Create the table `bigger_orders` using the columns from `orders` plus additional columns at the start and end:

    CREATE TABLE bigger_orders (
      another_orderkey bigint,
      LIKE orders,
      another_orderdate date
    )

Create a table with bloom indexes. Assuming orderkey has unique values: 

    CREATE TABLE IF NOT EXISTS sale_order(
        orderkey bigint,
        orderstatus varchar,
        totalprice double,
        orderdate date
    )
    with (format='ORC', transactional=true,orc_bloom_filter_columns=ARRAY['orderkey'], orc_bloom_filter_fpp=0.001);


Limitations
-----------

Different connector might support different data type, and different table/column properties. See connector documentation for more details.

See Also
--------

[ALTER TABLE](./alter-table.md), [DROP TABLE](./drop-table.md), [CREATE TABLE AS](./create-table-as.md), [SHOW CREATE TABLE](./show-create-table.md)

