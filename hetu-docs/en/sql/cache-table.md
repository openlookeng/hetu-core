
CACHE TABLE
===========

Synopsis
--------

``` sql
CACHE TABLE table_name WHERE condition;
```

Description
-----------

`CACHE TABLE` updates coordinator metadata with table name and partition that should be cached by the connector.

`condition` must be provided and is defined on only partition column(s) at this time. 

Examples
--------

Cache all sales data for stores located in 'CA':

``` sql
    CACHE TABLE store_sales WHERE location = 'CA';
```
Cache all sales data from stores sold after 20 Feb 2020:
 
``` sql 
    CACHE TABLE store_sales WHERE ss_sold_date_sk > 20200220;
```
Cache data with complex predicate string: 

```sql
    CACHE TABLE store_sales WHERE location = 'CA' AND ss_sold_date_sk > 20200220;
```

Limitations
-----------

- Only Hive connector (ORC Format) support this functionality at this time. See connector documentation for more details.
- Does not support `LIKE` in `WHERE` clause.
- Does not support 'OR' operator in complex predicate.

See Also
--------

[SHOW CACHE](./show-cache.md), [DROP CACHE](./drop-cache.md)
