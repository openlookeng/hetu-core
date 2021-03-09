
MySQL Connector
===============

The MySQL connector allows querying and creating tables in an external MySQL database. This can be used to join data between different systems like MySQL and Hive, or between two different MySQL instances.

Configuration
-------------

To configure the MySQL connector, create a catalog properties file in`etc/catalog` named, for example, `mysql.properties`, to mount the MySQL connector as the `mysql` catalog. Create the file with the following
contents, replacing the connection properties as appropriate for your setup:

``` properties
connector.name=mysql
connection-url=jdbc:mysql://example.net:3306
connection-user=root
connection-password=secret
```

- Whether to enable the query pushdown function

The pushdown function of the MySQL connector is enabled by default, and you do not need to perform any operation. You can also set the parameter as follows:

``` properties
jdbc.pushdown-enabled=true
#true indicates that pushdown is enabled, and false indicates that pushdown is disabled.
```

- Mode for the push-down feature

By default, the push-down mode of the MySQL connector is BASE_PUSHDOWN. If you want to enable all push-down, you can also set the parameter as follows:

``` properties
jdbc.pushdown-module=FULL_PUSHDOWN  
#FULL_PUSHDOWN: All push down. BASE_PUSHDOWN: Partial push down, which indicates that filter, aggregation, limit, topN and project can be pushed down.
```

### Multiple MySQL Servers

You can have as many catalogs as you need, so if you have additional MySQL servers, simply add another properties file to `etc/catalog` with a different name (making sure it ends in `.properties`). For example, if
you name the property file `sales.properties`, openLooKeng will create a catalog named `sales` using the configured connector.

Querying MySQL
--------------

The MySQL connector provides a schema for every MySQL *database*. You can see the available MySQL databases by running `SHOW SCHEMAS`:

    SHOW SCHEMAS FROM mysql;

If you have a MySQL database named `web`, you can view the tables in this database by running `SHOW TABLES`:

    SHOW TABLES FROM mysql.web;

You can see a list of the columns in the `clicks` table in the `web` database using either of the following:

    DESCRIBE mysql.web.clicks;
    SHOW COLUMNS FROM mysql.web.clicks;

Finally, you can access the `clicks` table in the `web` database:

    SELECT * FROM mysql.web.clicks;

If you used a different name for your catalog properties file, use that catalog name instead of `mysql` in the above examples.

MySQL Connector Limitations
---------------------------

The following SQL statements are not yet supported:

[DELETE](../sql/delete.md), [GRANT](../sql/grant.md), [REVOKE](../sql/revoke.md), [SHOW GRANTS](../sql/show-grants.md), [SHOW ROLES](../sql/show-roles.md), [SHOW ROLE GRANTS](../sql/show-role-grants.md)
