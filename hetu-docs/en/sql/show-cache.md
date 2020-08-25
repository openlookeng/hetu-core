
SHOW CACHE
==========

Synopsis
--------

``` sql
SHOW CACHE;
```

Description
-----------

`SHOW CACHE` displays the Split cache coordinator metadata. Split cache contains information about table and partition information that are cached.

Examples
--------

Show all cache metadata
 
```sql
    SHOW CACHE;
```

Show cache metadata for sales table

```sql
    SHOW CACHE sales;
```  

Limitations
-----------

Only Hive connector support this functionality at this time. See connector documentation for more details.

See Also
--------

[CACHE TABLE](./cache-table.md), [DROP CACHE](./drop-cache.md)
