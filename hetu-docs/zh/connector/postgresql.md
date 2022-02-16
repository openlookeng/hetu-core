
# PostgreSQL连接器

PostgreSQL连接器允许在外部PostgreSQL数据库中查询和创建表。这可用于在PostgreSQL和Hive等不同系统之间或在两个不同的PostgreSQL实例之间联接数据。

## 配置

要配置PostgreSQL连接器，在`etc/catalog`中创建一个目录属性文件，例如`postgresql.properties`，将PostgreSQL连接器挂载为`postgresql`目录。使用以下内容创建文件，并根据设置替换连接属性：

``` properties
connector.name=postgresql
connection-url=jdbc:postgresql://example.net:5432/database
connection-user=root
connection-password=secret
```

### 多个PostgreSQL数据库或服务器

PostgreSQL连接器只能访问PostgreSQL服务器中的单个数据库。因此，如果有多个PostgreSQL数据库，或者想要连接到多个PostgreSQL服务器，则必须配置多个PostgreSQL连接器实例。

要添加另一个目录，只需添加另一个属性文件到具有不同名称的`etc/catalog`中（确保它以`.properties`结尾）。例如，如果将属性文件命名为`sales.properties`，openLooKeng将使用配置的连接器创建一个名为`sales`的目录。

## 查询PostgreSQL

PostgreSQL连接器为每个PostgreSQL模式提供一个模式。可通过执行`SHOW SCHEMAS`来查看可用的PostgreSQL模式：

    SHOW SCHEMAS FROM postgresql;

如果有一个名为`web`的PostgreSQL模式，那么可以通过执行`SHOW TABLES`来查看这个模式中的表：

    SHOW TABLES FROM postgresql.web;

可以使用以下方法之一查看数据库`web`中`clicks`表中的列的列表：

    DESCRIBE postgresql.web.clicks;
    SHOW COLUMNS FROM postgresql.web.clicks;

最后，可以访问`web`模式中的`clicks`表：

    SELECT * FROM postgresql.web.clicks;

如果对目录属性文件使用不同的名称，请使用该目录名称，而不要使用上述示例中的`postgresql`。

## PostgreSQL Update/Delete 支持

### 使用PostgreSQL连接器创建表

示例：

```sql
CREATE TABLE postgresql_table (
    id int,
    name varchar(255));
```

### 对表执行INSERT

示例：

```sql
INSERT INTO postgresql_table
  VALUES
     (1, 'Jack'),
     (2, 'Bob');
```

### 对表执行UPDATE

示例：

```sql
UPDATE postgresql_table
  SET name='Tim'
  WHERE id=1;
```

上述示例将列`id`中值为`1`所在行的列`name`的值更新为`Tim`。

UPDATE前的SELECT结果：

```sql
lk:default> SELECT * FROM postgresql_table;
id | name
----+------
  1 | Jack
  2 | Bob
(2 rows)
```

UPDATE后的SELECT结果

```sql
lk:default> SELECT * FROM postgresql_table;
 id | name
----+------
  2 | Bob
  1 | Tim
(2 rows)
```

### 对表执行DELETE

示例：

```sql
DELETE FROM postgresql_table
  WHERE id=2;
```

以上示例删除了值为`2`的列`id`的行。

DELETE前的SELECT结果：

```sql
lk:default> SELECT * FROM postgresql_table;
 id | name
----+------
  2 | Bob
  1 | Tim
(2 rows)
```

DELETE后的SELECT结果：

```sql
lk:default> SELECT * FROM postgresql_table;
 id | name
----+------
  1 | Tim
(1 row)
```

## PostgreSQL连接器限制

暂不支持以下SQL语句：

[GRANT](../sql/grant.md)、[REVOKE](../sql/revoke.md)、[SHOW GRANTS](../sql/show-grants.md)、[SHOW ROLES](../sql/show-roles.md)、[SHOW ROLE GRANTS](../sql/show-role-grants.md)