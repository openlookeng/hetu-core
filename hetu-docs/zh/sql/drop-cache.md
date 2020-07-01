DROP CACHE
==========

Synopsis
--------

``` sql
DROP CACHE table
```

Description
-----------

`DROP CACHE` deletes cache metadata of the `table` from coordinator.

Examples
--------

Drop cache :

    DROP CACHE table

Limitations
-----------

Only Hive connector support this functionality. See connector documentation for more details.

See Also
--------

[cache-table](./cache-table.html), [show-cache](./show-cache.html)

