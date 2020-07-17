
ALTER SCHEMA
============

Synopsis
--------

``` sql
ALTER {SCHEMA|DATABASE} name RENAME TO new_name
```

Description
-----------

Change the definition of an existing schema.

Examples
--------

Rename schema `web` to `traffic`:

    ALTER SCHEMA web RENAME TO traffic
    ALTER DATABASE web RENAME TO traffic

Limitations
-----------

Some connectors do not support renaming schema, such as Hive Connector. See connector documentation for more details.

See Also
--------

[CREATE SCHEMA](./create-schema.md)
