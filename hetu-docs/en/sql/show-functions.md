
SHOW FUNCTIONS
==============

Synopsis
--------

``` sql
SHOW FUNCTIONS [ LIKE pattern [ ESCAPE 'escape_character' ] ]
```

Description
-----------

List all the functions available for use in queries. The LIKE clause can be used to restrict the list of function names.


**Note**

*If you want to list the external functions, you need to set session's list_built_in_functions_only property to false.*