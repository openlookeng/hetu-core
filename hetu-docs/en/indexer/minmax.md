
# MinMax Index

MinMax simply keeps tracks of the largest and smallest value.
The size of the index is extremely small.
However, this index will only be useful if the table is sorted
on the indexed column.

## Filtering

1. MinMax Index is used on coordinator for filtering splits during scheduling

## Selecting column for MinMax Index

MinMax Index will only work well on columns on which the table is sorted.
For example, ID or age.

## Supported operators

    =       Equality
    >       Greater than
    >=      Greater than or equal
    <       Less than
    <=      Less than or equal

## Supported column types
    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date"

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