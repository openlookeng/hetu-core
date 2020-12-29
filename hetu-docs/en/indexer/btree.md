# BTree Index

BTree Index utilizes the B-Tree data structure.
The size of the index increases as the number
of unique values in the column increases.

## Filtering

1. BTree Index is used on coordinator for filtering splits during scheduling

## Selecting column for BTree Index

BTree Index works on columns that have high cardinality (i.e. unique values),
such as an ID column, additionally it requires that the table be partitioned,
e.g. by date.

When selecting between BTree Index, the following should be considered:
- Bloom index only supports `=`
- Btree index requires the table to be partitioned
- Bloom index is probabilistic, whereas Btree index is deterministic. This means Btree will perform better filtering.
- Btree index size will be larger than Bloom index

## Supported operators

    =       Equality
    >       Greater than
    >=      Greater than or equal
    <       Less than
    <=      Less than or equal
    BETWEEN Between range
    IN      IN set

## Supported column types
    "integer", "smallint", "bigint", "tinyint", "varchar", "real", "date"

## Examples

Creating index:
```sql
create index idx using btree on hive.hindex.orders (orderid) with (level=partition) where orderDate='01-10-2020' ;
create index idx using btree on hive.hindex.orders (orderid) with (level=partition) where orderDate in ('01-10-2020', '01-10-2020');
```

* assuming orders table is partitioned on `orderDate`; table must be partitioned

Using index:
```sql
select * from hive.hindex.orders where orderid=12345
select * from hive.hindex.orders where orderid>12345
select * from hive.hindex.orders where orderid<12345
select * from hive.hindex.orders where orderid>=12345
select * from hive.hindex.orders where orderid<=12345
select * from hive.hindex.orders where orderid between (10000, 20000)
select * from hive.hindex.orders where orderid in (12345, 7890)
```