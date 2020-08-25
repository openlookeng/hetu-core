
DROP CACHE
==========

Synopsis
--------

``` sql
DROP CACHE table WHERE condition;
```

Description
-----------

`DROP CACHE` deletes cache metadata of the `table` from coordinator only. Workers\' caches purged automatically by expiry time 
or by reaching size limit but recurring splits will not reuse any cached node assignments.

Examples
--------
`DROP CACHE` command supports dropping specific cache records by matching the corresponding predicate string.
For example, if `SHOW CACHE` command shows that there is a record with predicate string
of `sale_id = 24` under the table of `sales`, then running the following query can delete it without
affecting any other cache record:

```sql
    DROP CACHE sales WHERE sale_id = 24;
```
Or simply drop all cache metadata for \'sales\' table by running:

```sql
    DROP CACHE sales;
```

Limitations
-----------

Only Hive connector support this functionality at this time. See connector documentation for more details.

See Also
--------

[CACHE TABLE](./cache-table.md), [SHOW CACHE](./show-cache.md)

