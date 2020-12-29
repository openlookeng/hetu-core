
# Bloom Index

Bloom Index utilizes Bloom Filters and index size will be fairly small.

## Filtering

1. Bloom Index is used on coordinator for filtering splits during scheduling
2. Bloom Index is used on workers for filtering Stripes when reading ORC files

## Selecting column for Bloom Index

Bloom Index works on columns that have high cardinality (i.e. unique values),
such as an ID column.

## Supported operators

    =       Equality

## Supported column types
    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date"

## Configurations

### `bloom.fpp`
 
> -   **Type:** `Double`
> -   **Default value:** `0.001`
> 
> Changes the FPP (false positive probability) value of the Bloom filter.
> Making this value smaller will increase the effectiveness of the index but
> will also increase the index size. The default value should be sufficient
> in most usecases. If the index is too large, this value can be increased
> e.g. 0.05.

## Examples

Creating index:
```sql
create index idx using bloom on hive.hindex.users (id);
create index idx using bloom on hive.hindex.users (id) where regionkey=1;
create index idx using bloom on hive.hindex.users (id) where regionkey in (3, 1);
create index idx using bloom on hive.hindex.users (id) WITH ("bloom.fpp" = '0.001');
```

* assuming users table is partitioned on `regionkey`

Using index:
```sql
select name from hive.hindex.users where id=123
```