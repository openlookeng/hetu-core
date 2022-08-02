The MariaDB connector allows querying and creating tables in external MariaDB databases. This can be used to join data between different systems such as MySQL and MariaDB or between two different MariaDB instances.

## Configuration

To configure the MariaDB connector, create a catalog properties file in `etc/catalog` in the openLooKeng installation directory, for example `maria.properties`, and mount the MariaDB connector as the maria directory. Create the file using the following and replace the connection properties according to the settings.

```properties
connector.name=maria
connection-url=jdbc:mariadb://Deployed MairaDB ip:MariaDB service port (default 3306)
connection-user=Your data source user
connection-password=Your data source password
```

- Whether to enable the query push-down feature

If you want to enable the connector push-down feature of MariaDB connector, you don't need to do anything, the MariaDB connector push-down feature is turned on by default. However, it can also be set as follows.

```properties
#true means push down is turned on, false means off.
jdbc.pushdown-enabled=true
```

- Push down mode selection

The default push-down mode of MariaDB connector is partial push-down, if you want to enable the full push-down function of MariaDB connector, you can set it as follows.

```properties
#FULL_PUSHDOWN, means all push down; BASE_PUSHDOWN, means partial push down, where partial push down means filter/aggregation/limit/topN/project which can be pushed down.
jdbc.pushdown-module=FULL_PUSHDOWN
```

### External function registration

The MariaDB connector supports registration of external functions.

Configure the external function registration namespace `catalog.schema` that supports push-down. For example, in `etc/catalog/maria.properties` configure.

```properties
jdbc.pushdown.remotenamespace=mariafun.default
```

### External function push-down

Push down external functions to MariaDB data sources for execution.

Configure the external function registration namespace `catalog.schema` that supports push-down. For example, in `etc/catalog/maria.properties` configure.

```properties
jdbc.pushdown.remotenamespace=mariafun.default
```

You can declare that you support functions in multiple function namespaces, just use '|' split in the `jdbc.pushdown.remotenamespace` configuration item. For example

```properties
# indicates that the current Connector instance supports both mysqlfun1.default, mysqlfun2.default, and mysqlfun3.default function namespaces final function push down to the currently connected data source for execution.
jdbc.pushdown.remotenamespace=mariafun1.default|mariafun2.default|mariafun3.default
```

### Multiple MariaDB servers

As many catalogs as needed can be created, so if there are additional MariaDB servers, just add another properties file with a different name to `etc/catalog` (make sure it ends with `.properties`). For example, if the properties file is named `sales.properties`, openLooKeng will create a catalog named `sales` using the configured connector.

## Querying MariaDB

The MariaDB Connector provides a schema for each MariaDB database. Available MariaDB databases can be viewed by executing `SHOW SCHEMAS`

```sql
-- The catalog name for this case is called maria
SHOW SCHEMAS FROM maria;
```

If you have a MariaDB database named `test`, you can view the tables in the database by executing `SHOW TABLES`

```sql
SHOW TABLES FROM maria.test;
```

You can view a list of columns in the `user` table in the `test` database using one of the following methods

```sql
DESCRIBE maria.test.user;
SHOW COLUMNS FROM maria.test.user;
```

Finally, you can access the `user` table in the `test` database

```sql
SELECT * FROM maria.test.user;
```

If you use a different name for the directory properties file, use that directory name instead of `maria` in the above example.

## MariaDB Connector Restrictions

The following SQL statements are not supported at this time

[DELETE](../sql/delete.md)、[GRANT](../sql/grant.md)、[REVOKE](../sql/revoke.md)、[SHOW GRANTS](../sql/show-grants.md)、[SHOW ROLES](../sql/show-roles.md)、[SHOW ROLE GRANTS](../sql/show-role-grants.md)