

# Usage

Index can be managed using any of the supported clients, such as hetu-cli located under the `bin` directory in the installation.


## CREATE
To create an index you can run sql statements of the form:
```roomsql  
CREATE INDEX [ IF NOT EXISTS ] index_name
USING [ BITMAP | BLOOM | BTREE | MINMAX ]
ON tbl_name (col_name)
WITH ( 'level' = ['STRIPE', 'PARTITION'], "autoload" = true, "bloom.fpp" = '0.001', "bloom.mmapEnabled" = false, [, …] )
WHERE predicate;
```

- `WHERE` predicate can be used to create index on select partition(s)
- `WITH` can be used to specify index properties or index level. See individual index documentation to support properties.
- `"level"='STRIPE'` if not specified
- `"autoload"` overrides the default value `hetu.heuristicindex.filter.cache.autoload-default` in config.properties. 
After the index is created or updated, whether to automatically load it into cache.
If false, index will be loaded as needed. This means, the first few queries may not benefit from index as it is being loaded into cache. 
Setting this to true may result in high memory usage but will give the best results.

If the table is partitioned, you can specify a single partition to create an index on, or an in-predicate to specify multiple partitions:

```roomsql
CREATE INDEX index_name USING bloom ON hive.schema.table (column1);
CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01") WHERE p=part1;
CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WHERE p in (part1, part2, part3);
```

**Note:** If the table is multi-partitioned (for example, partitioned by colA and colB), for BTree index, only index creation on the **first** level is supported (colA). Bloom, Bitmap and Minmax index creation on either (colA or colB) is supported.

## SHOW

To show the information of all the indices or a specific index by index_name.
The information includes index name, user, table name, index columns, index type, index status, etc.

```roomsql
SHOW INDEX;
SHOW INDEX index_name;
```

## UPDATE

Update an existing index if the source table has been modified. You can check the status of the index with ```SHOW INDEX index_name```.

```roomsql
UPDATE INDEX index_name;
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
The index will be automatically dropped if the source table is dropped.

## Notes on resource usage

### Disk usage
Heuristic index uses the local temporary directory (default `/tmp` on linux) while creating and processing indexes while running.
Therefore, the temporary directory should have sufficient space. To change the temporary directory set the following property in `etc/jvm.config`:

```
-Djava.io.tmpdir=/path/to/another/dir
```


