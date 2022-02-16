
openGauss Connector
====================

The openGauss connector allows querying and creating tables in an external openGauss database. This can be used to join data between different systems like openGauss and Hive, or between two different openGauss instances.

Configuration
-------------

To configure the openGauss connector, create a catalog properties file in `etc/catalog` named, for example, `opengauss.properties`, to mount the openGauss connector as the `opengauss` catalog. Create the file with the following contents, replacing the connection properties as appropriate for your setup:

``` properties
connector.name=openguass
connection-url=jdbc:postgresql://example.net:5432/database
connection-user=root
connection-password=secret
```

### Multiple openGauss Databases or Servers

The openGauss connector can only access a single database within a openGauss server. Thus, if you have multiple openGauss databases, or want to connect to multiple openGauss servers, you must configure
multiple instances of the openGauss connector.

To add another catalog, simply add another properties file to `etc/catalog` with a different name (making sure it ends in `.properties`). For example, if you name the property file `sales.properties`, openLooKeng will create a catalog named `sales` using the configured connector.

Querying openGauss
-------------------

The openGauss connector provides a schema for every openGauss schema. You can see the available openGauss schemas by running `SHOW SCHEMAS`:

    SHOW SCHEMAS FROM opengauss;

If you have a openGauss schema named `public`, you can view the tables in this schema by running `SHOW TABLES`:

    SHOW TABLES FROM opengauss.public;

You can see a list of the columns in the `hetutb` table in the `public` database using either of the following:

    DESCRIBE opengauss.public.hetutb;
    SHOW COLUMNS FROM opengauss.public.hetutb;

Finally, you can access the `hetutb` table in the `public` schema:

    SELECT * FROM opengauss.public.hetutb;

If you used a different name for your catalog properties file, use that catalog name instead of `opengauss` in the above examples.

## openGauss Update/Delete Support

### Create openGauss Table

Example：

```sql
CREATE TABLE opengauss_table (
    id int,
    name varchar(255));
```

### INSERT on openGauss tables

Example：

```sql
INSERT INTO opengauss_table
  VALUES
     (1, 'Jack'),
     (2, 'Bob');
```

### UPDATE on openGauss tables

Example：

```sql
UPDATE opengauss_table
  SET name='Tim'
  WHERE id=1;
```

Above example updates the column `name`'s value to `Tim` of rows with column `id` having value `1`.

SELECT result before UPDATE:

```sql
lk:default> SELECT * FROM opengauss_table;
id | name
----+------
  1 | Jack
  2 | Bob
(2 rows)
```

SELECT result after UPDATE

```sql
lk:default> SELECT * FROM opengauss_table;
id | name
----+------
2 | Bob
1 | Tim
(2 rows)
```

### DELETE on openGauss tables

Example：

```sql
DELETE FROM opengauss_table
  WHERE id=2;
```

Above example delete the rows with column `id` having value `2`.

SELECT result before DELETE:

```sql
lk:default> SELECT * FROM opengauss_table;
 id | name
----+------
  2 | Bob
  1 | Tim
(2 rows)
```

SELECT result after DELETE:

```sql
lk:default> SELECT * FROM opengauss_table;
 id | name
----+------
  1 | Tim
(1 row)
```

****Note:****

> - When the compatibility type of the openGuass database is O (DBCOMPATIBILITY = A), the `Date` data type is not supported.

> - The openGuass driver does not support put the connection in read-only mode to enable database optimization at present.

> - The unit of openGauss's `Character` data type is byte (e.g. `n` indicates to the byte length of `VARCHAR(n)` data type), the unit of openLooKeng's `Character` data type is character (e.g. `n` indicates to the character length of `VARCHAR(n)` data type). openGauss connector does not support use `create-table-as` method to create a table containing `Character` data type directly, the byte length of `Character` data type needs to manually specified.

> - The `use-connection-pool` configuration is not supported.

> - Setting `opengauss.metadata.speedup=true` in catalog properties file can speed up DatabaseMetaData access, its default value is false.

*If the subsequent version of openGuass supports the above restriction, we will make corresponding adaptation.*

openGauss Connector Limitations
--------------------------------

The following SQL statements are not yet supported:

[GRANT](../sql/grant.md), [REVOKE](../sql/revoke.md), [SHOW GRANTS](../sql/show-grants.md), [SHOW ROLES](../sql/show-roles.md), [SHOW ROLE GRANTS](../sql/show-role-grants.md)
