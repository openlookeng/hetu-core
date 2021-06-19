
REFRESH META CACHE
==================

Synopsis
--------

```sql
REFRESH META CACHE [FOR CATALOG]
```

Description
-----------

`REFRESH META CACHE` refreshes the current catalog connector metadata cache. `REFRESH META CACHE FOR CATALOG` refreshes the specified `catalog` connector metadata cache.

Examples
--------

Refresh the current catalog connector metadata cache

```sql
REFRESH META CACHE
```

Refresh the specified catalog connector metadata cache

```sql
REFRESH META CACHE FOR catalog
```

Limitations
-----------

Only Hive connector support this functionality at this time.
