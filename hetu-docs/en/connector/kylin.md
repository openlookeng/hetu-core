
Kylin Connector
===============

The Kylin connector allows querying tables in an external Kylin database. This can be used to join data between different systems like Kylin and Hive, or between two different Kylin instances.

Configuration
-------------

To configure the Kylin connector, create a catalog properties file in`etc/catalog` named, for example, `Kylin.properties`, to mount the Kylin connector as the `Kylin` catalog. Create the file with the following
contents, replacing the connection properties as appropriate for your setup:

``` properties
connector.name=kylin
connection-url=jdbc:kylin://example.net/project
connection-user=root
connection-password=secret
connector-planoptimizer-rule-blacklist=io.prestosql.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy
```

The connector-planoptimizer-rule-blacklist attribute is specially configured for kylin, and the default value is io.prestosql.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy

Multiple Kylin Servers
----------------------

You can have as many catalogs as you need, so if you have additional Kylin servers, simply add another properties file to `etc/catalog` with a different name (making sure it ends in `.properties`). For example, if
you name the property file `sales.properties`, openLooKeng will create a catalog named `sales` using the configured connector.

Querying Kylin
--------------

you can access the `clicks` table in the `web` database:

    SELECT * FROM Kylin.web.clicks;

If you used a different name for your catalog properties file, use that catalog name instead of `Kylin` in the above examples.

Kylin Connector Limitations
---------------------------

The following SQL statements are not yet supported:

[DELETE](../sql/insert.md)、../sql/update.md)、../sql/delete.md)、[GRANT](../sql/grant.md)、[REVOKE](../sql/revoke.md)、[SHOW GRANTS](../sql/show-grants.md)、[SHOW ROLES](../sql/show-roles.md)、[SHOW ROLE GRANTS](../sql/show-role-grants.md)