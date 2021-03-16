Greenplum Connector
====================

The Greenplum connector allows querying and creating tables in an external Greenplum database.
This can be used to join data between different systems like Greenplum and Hive,
 or between two different Greenplum instances.
The Greenplum connector is extended from PostgreSQL connector, and We enable query push down feature for Greenplum connector.

Configuration
-------------

The base configuration of Greenplum connector is the same as PostgreSQL connector.
For example, you can mount the Greenplum connector by creating a file named `greenplum.properties` in configuration directory `etc/catalog`. And create the file with the following contents,
replacing the connection properties with correct string.

``` properties
connector.name=greenplum
connection-url=jdbc:postgresql://example.net:5432/database
connection-user=root
connection-password=secret
```

### Table modification

We can set the `greenplum.allow-modify-table` in the `greenplum.properties` to allow to modify the table or not.
If `greenplum.allow-modify-table` is set to `false`, you can not create/rename/add column/rename column/drop column.
And `greenplum.allow-modify-table` is set to `true` by default.

``` properties
greenplum.allow-modify-table=true
```

### Enable query push down in Greenplum connector

The query push down feature help you to push filter, project or other sql operators down to the Greenplum database
which can reduce data transmission volume between openLooKeng and Greenplum data source.
If `jdbc.pushdown-enabled` is set to `false`, query push down is disabled.
Or `jdbc.pushdown-enabled` is set to `true`, query push down is enabled. The query push down feature is turn on by default.
For example, to disable query push down, you can set connection properties in the catalog file as follows:

``` properties
jdbc.pushdown-enabled=false
```

- Mode for the push-down feature

By default, the push-down mode of the Greenplum connector is `BASE_PUSHDOWN`. If you want to enable all push-down, you can also set the parameter as follows:

``` properties
jdbc.pushdown-module=FULL_PUSHDOWN  
#FULL_PUSHDOWN: All push down. BASE_PUSHDOWN: Partial push down, which indicates that filter, aggregation, limit, topN and project can be pushed down.
```

### Others
About the other configurations and how to query greenplum, You can refer to the document of PostgreSQL connector for more details.
 