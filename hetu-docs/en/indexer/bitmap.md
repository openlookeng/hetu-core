
# Bitmap Index

Bitmap Index utilizes Bitmaps. The size of the index increases as the number
of unique values in the column increases. For example, a column like gender
will have a small size. Whereas a column like ID will have an extremely 
large size (not recommended).

Note: Bitmap Index can additionally benefit when ORC predicate pushdown is enabled.
This can be enabled by setting `hive.orc-predicate-pushdown-enabled=true`
in `hive.properties` or setting the session using `set session hive.orc_predicate_pushdown_enabled=true;`. 
Setting this to true will improve improve the performance of queries that utilize Bitmap Index.
See [Properties](../admin/properties.md) for details.

## Filtering

1. Bitmap Index is used on workers for filtering rows when reading ORC files.

## Selecting column for Bitmap Index

Bitmap Index works on columns that have a low cardinality (i.e. few unique values),
such as a Gender column.

## Supported operators

    =       Equality
    >       Greater than
    >=      Greater than or equal
    <       Less than
    <=      Less than or equal
    BETWEEN Between range
    IN      IN set

## Supported column types
    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date"

## Examples

Creating index:
```sql
create index idx using bitmap on hive.hindex.users (gender);
create index idx using bitmap on hive.hindex.users (gender) where regionkey=1;
create index idx using bitmap on hive.hindex.users (gender) where regionkey in (3, 1);
```

* assuming users table is partitioned on `regionkey`

Using index:
```sql
select name from hive.hindex.users where gender="female"
select * from hive.hindex.users where id>123
select * from hive.hindex.users where id<123
select * from hive.hindex.users where id>=123
select * from hive.hindex.users where id<=123
select * from hive.hindex.users where id between (100, 200)
select * from hive.hindex.users where id in (123, 199)
```