
# MinMaxIndex

MinMaxIndex simply keeps tracks of the largest and smallest value.
The size of the index is extremely small.
However, this index will only be useful if the table is sorted
on the indexed column.

## Use case(s)

**Note: Currently, Heuristic Index only supports the Hive connector with 
tables using ORC storage format.**

MinMaxIndex is used on coordinator for filtering splits during scheduling.

## Selecting column for MinMaxIndex

Queries that have a filter predicate on a column on which data is sorted
can benefit from MinMaxIndex.

For example, a query like `SELECT name FROM users WHERE age>25`
can benefit from having a MinMaxIndex on the `age` column if
the data is sorted on `age` column.

## Supported operators

    =       Equality
    >       Greater than
    >=      Greater than or equal
    <       Less than
    <=      Less than or equal

## Supported column types
    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date", "decimal"

**Note:** Index cannot be created on unsupported data types.

## Examples

Creating index:
```sql
create index idx using minmax on hive.hindex.users (age);
create index idx using minmax on hive.hindex.users (age) where regionkey=1;
create index idx using minmax on hive.hindex.users (age) where regionkey in (3, 1);
```

* assuming users table is partitioned on `regionkey`

Using index:
```sql
select name from hive.hindex.users where age=20
select name from hive.hindex.users where age>20
select name from hive.hindex.users where age<20
select name from hive.hindex.users where age>=20
select name from hive.hindex.users where age<=20
```