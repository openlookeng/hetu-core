DROP SCHEMA
===========

Synopsis
--------

``` sql
DROP {SCHEMA|DATABASE} [ IF EXISTS ] schema_name [{CASCADE | RESTRICT}]
```

Description
-----------

Drop an existing schema. The schema must be empty.

The optional `IF EXISTS` clause causes the error to be suppressed if the schema does not exist.

Examples
--------

Drop the schema `web`:

    DROP SCHEMA web
    DROP DATABASE web

Drop the schema `sales` if it exists:

    DROP TABLE IF EXISTS sales

Limitations
-----------

Functionally, `CASCADE` and `RESTRICT` is not supported yet.

See Also
--------

[alter-schema](./alter-schema.html), [create-schema](./create-schema.html)