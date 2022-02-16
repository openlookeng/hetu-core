
PostgreSQL Connector
====================

The PostgreSQL connector allows querying and creating tables in an external PostgreSQL database. This can be used to join data between different systems like PostgreSQL and Hive, or between two different
PostgreSQL instances.

Configuration
-------------

To configure the PostgreSQL connector, create a catalog properties file in `etc/catalog` named, for example, `postgresql.properties`, to mount the PostgreSQL connector as the `postgresql` catalog. Create the file with the following contents, replacing the connection properties as appropriate for your setup:

``` properties
connector.name=postgresql
connection-url=jdbc:postgresql://example.net:5432/database
connection-user=root
connection-password=secret
```

### Multiple PostgreSQL Databases or Servers

The PostgreSQL connector can only access a single database within a PostgreSQL server. Thus, if you have multiple PostgreSQL databases, or want to connect to multiple PostgreSQL servers, you must configure
multiple instances of the PostgreSQL connector.

To add another catalog, simply add another properties file to `etc/catalog` with a different name (making sure it ends in `.properties`). For example, if you name the property file `sales.properties`, openLooKeng will create a catalog named `sales` using the configured connector.

Querying PostgreSQL
-------------------

The PostgreSQL connector provides a schema for every PostgreSQL schema. You can see the available PostgreSQL schemas by running `SHOW SCHEMAS`:

    SHOW SCHEMAS FROM postgresql;

If you have a PostgreSQL schema named `web`, you can view the tables in this schema by running `SHOW TABLES`:

    SHOW TABLES FROM postgresql.web;

You can see a list of the columns in the `clicks` table in the `web` database using either of the following:

    DESCRIBE postgresql.web.clicks;
    SHOW COLUMNS FROM postgresql.web.clicks;

Finally, you can access the `clicks` table in the `web` schema:

    SELECT * FROM postgresql.web.clicks;

If you used a different name for your catalog properties file, use that catalog name instead of `postgresql` in the above examples.

## PostgreSQL Update/Delete Support

### Create PostgreSQL Table

Example：

```sql
CREATE TABLE postgresql_table (
    id int,
    name varchar(255));
```

### INSERT on PostgreSQL tables

Example：

```sql
INSERT INTO postgresql_table
  VALUES
     (1, 'Jack'),
     (2, 'Bob');
```

### UPDATE on PostgreSQL tables

Example：

```sql
UPDATE postgresql_table
  SET name='Tim'
  WHERE id=1;
```

Above example updates the column `name`'s value to `Tim` of rows with column `id` having value `1`.

SELECT result before UPDATE:

```sql
lk:default> SELECT * FROM postgresql_table;
id | name
----+------
  1 | Jack
  2 | Bob
(2 rows)
```

SELECT result after UPDATE

```sql
lk:default> SELECT * FROM postgresql_table;
id | name
----+------
2 | Bob
1 | Tim
(2 rows)
```

### DELETE on PostgreSQL tables

Example：

```sql
DELETE FROM postgresql_table
  WHERE id=2;
```

Above example delete the rows with column `id` having value `2`.

SELECT result before DELETE:

```sql
lk:default> SELECT * FROM postgresql_table;
 id | name
----+------
  2 | Bob
  1 | Tim
(2 rows)
```

SELECT result after DELETE:

```sql
lk:default> SELECT * FROM postgresql_table;
 id | name
----+------
  1 | Tim
(1 row)
```

PostgreSQL Connector Limitations
--------------------------------

The following SQL statements are not yet supported:

[GRANT](../sql/grant.md), [REVOKE](../sql/revoke.md), [SHOW GRANTS](../sql/show-grants.md), [SHOW ROLES](../sql/show-roles.md), [SHOW ROLE GRANTS](../sql/show-role-grants.md)