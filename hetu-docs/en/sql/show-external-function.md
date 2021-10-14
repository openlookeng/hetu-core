
SHOW EXTERNAL FUNCTION
======================

Synopsis
--------

``` sql
SHOW EXTERNAL FUNCTION function_name [ ( parameter_type[, ...] ) ]
```

Description
-----------
Show the `external function` information which have been registered in our system. 
If parameter type list is omitted, show one row for each signature with the given `function_name`.

Example
-------

Show the `external function` information of `example.default.format(double, integer)`:

``` sql
show external function example.default.format(double, integer);
```
``` sql
                                                             External Function                                                              | Argument Types
--------------------------------------------------------------------------------------------------------------------------------------------+-----------------
 External FUNCTION example.default.format (                                                                                                 | double, integer
    k double,                                                                                                                               |
    h integer                                                                                                                               |
 )                                                                                                                                          |
 RETURNS varchar                                                                                                                            |
                                                                                                                                            |
 COMMENT 'format the number ''num'' to a format like''#,###,###.##'', rounded to ''lo'' decimal places, and returns the result as a string' |
 DETERMINISTIC                                                                                                                              |
 RETURNS NULL ON NULL INPUT                                                                                                                 |
 EXTERNAL  
```

Show the `external function` information of `example.default.format`:

``` sql
show external function example.default.format;
```
``` sql
                                                             External Function                                                              | Argument Types
--------------------------------------------------------------------------------------------------------------------------------------------+-----------------
External FUNCTION example.default.format (                                                                                                 | double, integer
  k double,                                                                                                                               |
  h integer                                                                                                                               |
)                                                                                                                                          |
RETURNS varchar                                                                                                                            |
                                                                                                                                          |
COMMENT 'format the number ''num'' to a format like''#,###,###.##'', rounded to ''lo'' decimal places, and returns the result as a string' |
DETERMINISTIC                                                                                                                              |
RETURNS NULL ON NULL INPUT                                                                                                                 |
EXTERNAL  
External FUNCTION example.default.format (                                                                                                | real, integer
  k real,                                                                                                                              |
  h integer                                                                                                                               |
)                                                                                                                                          |
RETURNS varchar                                                                                                                            |
                                                                                                                                          |
COMMENT 'format the number ''num'' to a format like''#,###,###.##'', rounded to ''lo'' decimal places, and returns the result as a string' |
DETERMINISTIC                                                                                                                              |
RETURNS NULL ON NULL INPUT                                                                                                                 |
EXTERNAL  
```

See Also
--------
[Function Namespace Managers](../admin/function-namespace-managers.md)

