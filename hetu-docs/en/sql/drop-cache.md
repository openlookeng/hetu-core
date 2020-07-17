
DROP CACHE
==========

Synopsis
--------

``` sql
DROP CACHE table
```

Description
-----------

`DROP CACHE` deletes cache metadata of the `table` from coordinator only. Workers' caches purged automatically by expiry time 
or by reaching size limit but recurring splits will not reuse any cached node assignments.

Examples
--------

Drop cache metadata for `sales' table
 
```sql 
    DROP CACHE sales
```

Limitations
-----------

Only Hive connector support this functionality at this time. See connector documentation for more details.

See Also
--------

[CACHE TABLE](./cache-table.md), [SHOW CACHE](./show-cache.md)

