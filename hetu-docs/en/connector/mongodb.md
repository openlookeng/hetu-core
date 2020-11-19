# MongoDB Connector

The MongoDB connector allows MongoDB collections to be used as tables in the openLooKeng.

**Note:**

    MongoDB 2.6 and later versions are supported, you are advised to use version 3.0 or later.

## Configuration

To configure the MongoDB connector, create a catalog property file `etc/catalog/mongodb.properties` by referring to the following content and replace the properties as required:

```properties
connector.name=mongodb
mongodb.seeds=host1,host:port
```

### Multiple MongoDB Clusters

Multiple catalogs can be created as required. Therefore, if there is an additional MongoDB cluster, you only need to add another property file with a different name to `etc/catalog` (ensure that it ends with `.properties`). For example, if you name the property file as `sales.properties`, the openLooKeng will create a catalog named `sales` using the configured connector.

## Configuring Properties

The following properties are available:

| Property Name| Description|
|----------|----------|
| `mongodb.seeds`| List of all mongod servers|
| `mongodb.schema-collection`| A collection of schema information|
| `mongodb.case-insensitive-name-matching`| Case-insensitive matching between database and collection names|
| `mongodb.credentials`| List of credentials|
| `mongodb.min-connections-per-host`| Minimum number of the connection pools per host|
| `mongodb.connections-per-host`| Maximum number of the connection pools per host|
| `mongodb.max-wait-time`| Maximum waiting time|
| `mongodb.max-connection-idle-time`| Maximum idle time of connection pooling|
| `mongodb.connection-timeout`| Communication connection timeout|
| `mongodb.socket-timeout`| Communication timeout|
| `mongodb.socket-keep-alive`| Whether to enable the keep-alive function for each communication channel|
| `mongodb.ssl.enabled`| Using TLS/SSL to connect to mongod/mongos|
| `mongodb.read-preference`| Read preference|
| `mongodb.write-concern`| Write policy|
| `mongodb.required-replica-set`| Name of the required replica set|
| `mongodb.cursor-batch-size`| Number of elements returned in a batch|

### `mongodb.seeds`

List of all mongod servers in the same replica set, which are separated using commas (,). The format is in `hostname[:port]`. Or list of mongos servers in the same sharded cluster. If port is not specified, port 27017 is used.

This property is mandatory. There is no default value, and at least one seed must be defined.

### `mongodb.schema-collection`

MongoDB is a document-oriented database, and there is no fixed schema information in the system. Therefore, a special collection in each MongoDB database should define the structure for all tables. For more information, see the [Table Definition](./mongodb.md#table-definition) section.

At startup, this connector attempts to guess the type of the field, but the type may not match the collection you created. In this case, you need to manually modify it. `CREATE TABLE` and `CREATE TABLE AS SELECT` are used to create an entry for you.

This property is optional. The default value is `_schema`.

### `mongodb.case-insensitive-name-matching`

Case-insensitive matching between database and collection names.

This property is optional. The default value is `false`.

### `mongodb.credentials`

List of `username:password@collection` credentials separated by commas (,).

This property is optional. There is no default value.

### `mongodb.min-connections-per-host`

Minimum number of connections per host in the MongoClient instance. These connections are retained in the connection pool when they are idle. Over time, the connection pool contains at least this minimum number of connections.

This property is optional. The default value is `0`.

### `mongodb.connections-per-host`

Maximum number of connections per host in the MongoClient instance. These connections are retained in the connection pool when they are idle. Once the connection pool resources are exhausted, any operation that requires a connection is blocked and waits for an available connection.

This property is optional. The default value is `100`.

### `mongodb.max-wait-time`

Maximum time (in milliseconds) that a thread can wait for a connection to become available. The value `0` indicates that the thread will not wait. A negative value indicates that the thread will wait will indefinitely for a connection to become available.

This property is optional. The default value is `120000`.

### `mongodb.connections-timeout`

Connection timeout interval (in milliseconds). The value `0` indicates that no timeout occurs. This property is used only when a new connection is set up.

This property is optional. The default value is `10000`.

### `mongodb.socket-timeout`

Socket timeout interval (in milliseconds). It is used for I/O socket read and write operations.

This property is optional. The default value is `0`, indicating that no timeout occurs.

### `mongodb.socket-keep-alive`

This property controls the socket keep-alive function, which keeps the connection alive through the firewall.

This property is optional. The default value is `false`.

### `mongodb.ssl.enabled`

This property is used to enable the SSL connection with the MongoDB server.

This property is optional. The default value is `false`.

### `mongodb.read-preference`

The read preference are used for query, mapping restoration, aggregation, and counting. The value can be `PRIMARY`, `PRIMARY_PREFERRED`, `SECONDARY`, `SECONDARY_PREFERRED`, or `NEAREST`.

This property is optional. The default value is `PRIMARY`.

### `mongodb.write-concern`

Write policy. The value can be `ACKNOWLEDGED`, `FSYNC_SAFE`, `FSYNCED`, `JOURNAL_SAFEY`, `JOURNALED`, `MAJORITY`, `NORMAL`, `REPLICA_ACKNOWLEDGED`, `REPLICAS_SAFE`, or `UNACKNOWLEDGED`.

This property is optional. The default value is `ACKNOWLEDGED`.

### `mongodb.required-replica-set`

Name of the required replica set. After this property is set, the MongoClient instance performs the following operations:

> - Connect in replica set mode and discover all members in the collection based on the specified server.
> - Ensure that the collection name reported by all members matches the required collection name.
> - If any member of the seed list is not part of a replica set with the required name, any requests are rejected.

This property is optional. There is no default value.

### `mongodb.cursor-batch-size`

Limits the number of elements returned in a batch. A cursor typically fetches a batch of result objects and stores them locally. If **batchSize** is set to **0**, the default value of the driver is used. If the value of **batchSize** is positive, the value indicates the size of each batch of objects that are retrieved. The value can be adjusted to optimize performance and limit data transfer. If the value of **batchSize** is negative, it will limit the number of returned objects to the maximum batch size (typically 4 MB), and the cursor will be closed. For example, if **batchSize** is **-10**, the server will return up to 10 documents, return as many documents as possible in 4 MB, and then close the cursor.

**Note**

    Do not set the batch size to 1.

This property is optional. The default value is `0`.

## Table Definition

MongoDB maintains the table definition on the configuration special collection specified by `mongodb.schema-collection`.

**Note**

    The plugin cannot detect a collection deletion.
    You need to run db.getCollection("_schema").remove({table: delete_table_name}) in the Mongo shell to delete the collection.
    Or you can delete the collection by running DROP TABLE table_name using openLooKeng.

A collection in a schema consists of MongoDB documents of a table.

    {
        "table": ...,
        "fields": [
              { "name" : ...,
                "type" : "varchar|bigint|boolean|double|date|array(bigint)|...",
                "hidden" : false },
                ...
            ]
        }
    }

| Field| Mandatory or Optional| Type| Description|
|:----------|:----------|:----------|:----------|
| `table`| Mandatory| string| Name of the openLooKeng table|
| `fields`| Mandatory| array| Field definition list. For each column definition, a new column is created in the openLooKeng table.|

The definition of each field is as follows:

    {
        "name": ...,
        "type": ...,
        "hidden": ...
    }

| Field| Mandatory or Optional| Type| Description|
|:----------|:----------|:----------|:----------|
| `name`| Mandatory| string| Name of a column in the openLooKeng table|
| `type`| Mandatory| string| Type of a column|
| `hidden`| Optional| boolean| Hides the column from the `DESCRIBE <table name>` and `SELECT *` results. The default value is `false`.|

There is no restriction on the field description of the key or message.

## ObjectId

The MongoDB collection has a special field `_id`. The connector attempts to follow the same rules for this special field, so there will be a hidden field `_id`.

```sql
    CREATE TABLE IF NOT EXISTS orders (
        orderkey bigint,
        orderstatus varchar,
        totalprice double,
        orderdate date
    );

    INSERT INTO orders VALUES(1, 'bad', 50.0, current_date);
    INSERT INTO orders VALUES(2, 'good', 100.0, current_date);
    SELECT _id, * FROM orders;
```

```sql
                     _id                 | orderkey | orderstatus | totalprice | orderdate
    -------------------------------------+----------+-------------+------------+------------
     55 b1 51 63 38 64 d6 43 8c 61 a9 ce |        1 | bad         |       50.0 | 2015-07-23
     55 b1 51 67 38 64 d6 43 8c 61 a9 cf |        2 | good        |      100.0 | 2015-07-23
    (2 rows)
```

```sql
    SELECT _id, * FROM orders WHERE _id = ObjectId('55b151633864d6438c61a9ce');
```

```sql
                     _id                 | orderkey | orderstatus | totalprice | orderdate
    -------------------------------------+----------+-------------+------------+------------
     55 b1 51 63 38 64 d6 43 8c 61 a9 ce |        1 | bad         |       50.0 | 2015-07-23
    (1 row)
```

The `_id` field can be rendered as a readable value and converted to `VARCHAR`:

```sql
    SELECT CAST(_id AS VARCHAR), * FROM orders WHERE _id = ObjectId('55b151633864d6438c61a9ce');
```

```sql

               _id             | orderkey | orderstatus | totalprice | orderdate
    ---------------------------+----------+-------------+------------+------------
     55b151633864d6438c61a9ce  |        1 | bad         |       50.0 | 2015-07-23
    (1 row)
```

## Restrictions

\- [Row deletion](../sql/delete.md) is not supported.

\- View creation is not supported.

\- The empty tables and fields created by the openLooKeng cannot be queried in the MongoDB.

\- After a table or field is deleted from the MongoDB, the table or field still exists in the openLooKeng while the values are NULL.

