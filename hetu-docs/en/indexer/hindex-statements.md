

# Usage

Index can be managed using any of the supported clients, such as hetu-cli located under the `bin` directory in the installation.


## CREATE
To create an index you can run sql statements of the form:
```roomsql  
CREATE INDEX [ IF NOT EXISTS ] index_name
USING [ BITMAP | BLOOM | BTREE | MINMAX ]
ON tbl_name (col_name)
WITH ( 'level' = ['STRIPE', 'PARTITION'], "bloom.fpp" = '0.001', [, …] )
WHERE predicate;
```

- `WHERE` predicate can be used to create index on select partition(s)
- `WITH` can be used to specify index properties or index level. See individual index documentation to support properties.
- `"level"='STRIPE'` if not specified

If the table is partitioned, you can specify a single partition to create an index on, or an in-predicate to specify multiple partitions: 

```roomsql
CREATE INDEX index_name USING bloom ON hive.schema.table (column1);
CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01") WHERE p=part1;
CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WHERE p in (part1, part2, part3);
```

## SHOW

To show all indexes or a specific index_name: 
```roomsql
SHOW INDEX;
SHOW INDEX index_name;
```

## DROP

To delete an index by name:
```roomsql
DROP INDEX index_name
WHERE predicate;
```

- `WHERE` predicate be used to delete index for specific partition(s). However, if index was initially created on the entire table, it is not possible to delete index for a single partition.

```roomsql
DROP INDEX index_name where p=part1;
```

Note: dropped index will be removed from cache after a few seconds, so you may still see the next few queries still using the index.


## Notes on resource usage

### Disk usage
Heuristic index uses the local temporary directory (default `/tmp` on linux) while creating and processing indexes while running.
Therefore, the temporary directory should have sufficient space. To change the temporary directory set the following property in `etc/jvm.config`:

```
-Djava.io.tmpdir=/path/to/another/dir
```

The size of the index depends closely on the column properties such as number of unique values.
As a rough estimate, the available temporary disk space should be table size divided by the number of columns.
For example, e.g. for a table of 100GB with five mostly unique columns, 25GB of temporary disk space should be available.

