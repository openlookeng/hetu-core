CACHE TABLE
===========

Synopsis
--------

``` sql
CACHE TABLE table_name WHERE condition
```

Description
-----------

`CACHE TABLE` updates coordinator metadata with table name and partition that should be cached by the connector.

`condition` must be provided and is defined on only partition column(s).

Examples
--------

Cache table :

    CACHE TABLE store_sales where location = 'CA';

Limitations
-----------

Only Hive connector support this functionality. See connector documentation for more details.

See Also
--------

[show-cache](./show-cache)
[drop-cache](./drop-cache)
