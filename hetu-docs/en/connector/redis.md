Redis Connector
====================
Overview
--------
this connector allows the use of Redis key/value pair is presented as a single row in openLooKeng.

**Note**

*In Redis,key/value pair can only be mapped to string or hash value types.keys can be stored in a zset,then keys can split into multiple slice*

*Support Redis 2.8.0 or higher*

Configuration
-------------
To configure the Redis connector, create a catalog properties file `etc/catalog/redis.properties` with the following contents, replacing the properties as appropriate:
``` properties
connector.name=redis
redis.table-names=schema1.table1,schema1.table2
redis.nodes=host1:port
```
### Multiple Redis Servers
You can have as many catalogs as you need. If you have additional
Redis servers, simply add another properties file to ``etc/catalog``
with a different name, making sure it ends in ``.properties``.
For example, if you name the property file `sales.properties`, openLooKeng will create a catalog named `sales` using the configured connector.

Configuration properties
------------------------
The following configuration properties are available:

| Property Name                                                    | Description                                                                                                           |
|:-----------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------|
| `redis.table-names`                                              | List of all tables provided by the catalog                                                                            |
| `redis.default-schema`                                           | Default schema name for tables  (default `default`)                                                                   |
| `redis.nodes`                                                    | List of nodes in the Redis server                                                                                     |
| `redis.connect-timeout`                                          | Timeout for connecting to the  Redis server (ms) (default 2000)                                                       |
| `redis.scan-count`                                               | The number of keys obtained from each scan for string and hash value types (default 100)                              |
| `redis.key-prefix-schema-table`                                  | Redis keys have schema-name:table-name prefix   (default  false)                                                      |
| `redis.key-delimiter`                                            | Delimiter separating schema_name and table_name if redis.key-prefix-schema-table is used (default `:`)                |
| `redis.table-description-dir`                                    | Directory containing table description files (default `etc/redis/`)                                                   |
| `redis.hide-internal-columns`                                    | Whether internal columns are shown in table metadata or not. (default true)                                           |
| `redis.database-index`                                           | Redis database index  (default 0)                                                                                     |
| `redis.password`                                                 | Redis server password  (default null)                                                                                 |
| `redis.table-description-interval`                               | the interval of flush description files (ms) (default no flush,table description will be memoized without expiration) |


Internal columns
----------------

| Column name        | Type    | Description                                                                                                                              |
|:-------------------| :------ |:-----------------------------------------------------------------------------------------------------------------------------------------|
| `_key`             | VARCHAR  | Redis key.                                                                                                                               |
| `_value`           | VARCHAR   | Redis value corresponding to the key                                                                                                     |
| `_key_length`   | BIGINT  | Number of bytes in the key.                                                                                                              |
| `_key_corrupt`     | BOOLEAN  | True if the decoder could not decode the key for this row. When true, data columns mapped from the key should be treated as invalid.     |
| `_value_corrupt`   | BOOLEAN | True if the decoder could not decode the value for this row. When true, data columns mapped from the value should be treated as invalid. |


Table Definition Files
----------------------
For openLooKeng, every key/value pair  must be mapped into columns to allow queries against the data. It is like kafka conntector,so you can refer to kafka-tutorial

A table definition file consists of a JSON definition for a table. The name of the file can be arbitrary but must end in `.json`.

for example,there is a nation.json
``` json
{
    "tableName": "nation",
    "schemaName": "tpch",
    "key": {
        "dataFormat": "raw",
        "fields": [
            {
                "name": "redis_key",
                "type": "VARCHAR(64)",
                "hidden": "true"
            }
        ]
    },
    "value": {
        "dataFormat": "json",
        "fields": [
            {
                "name": "nationkey",
                "mapping": "nationkey",
                "type": "BIGINT"
            },
            {
                "name": "name",
                "mapping": "name",
                "type": "VARCHAR(25)"
            },
                        {
                "name": "regionkey",
                "mapping": "regionkey",
                "type": "BIGINT"
            },
            {
                "name": "comment",
                "mapping": "comment",
                "type": "VARCHAR(152)"
            }
       ]
    }
}
```
In redis,such data exists
```shell
127.0.0.1:6379> keys tpch:nation:*
 1) "tpch:nation:2"
 2) "tpch:nation:4"
 3) "tpch:nation:16"
 4) "tpch:nation:18"
 5) "tpch:nation:10"
 6) "tpch:nation:17"
 7) "tpch:nation:1"
```
```shell
127.0.0.1:6379> get tpch:nation:1
"{\"nationkey\":1,\"name\":\"ARGENTINA\",\"regionkey\":1,\"comment\":\"al foxes promise slyly according to the regular accounts. bold requests alon\"}"
```
Now we can use redis connector get data from redis,(redis_key don't show,because we set "hidden": "true" ) 
```shell
lk> select * from redis.tpch.nation;
 nationkey |      name      | regionkey |                                                      comment                                                       
-----------+----------------+-----------+--------------------------------------------------------------------------------------------------------------------
         3 | CANADA         |         1 | eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold              
         9 | INDONESIA      |         2 |  slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull 
        19 | ROMANIA        |         3 | ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account    
         2 | BRAZIL         |         1 | y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special   
```
**Note**

*if redis.key-prefix-schema-table is false (default is false),all keys in redis will be mapped to table's key,no matching occurs*

Please refer to the `kafka-tutorial` for the description of the ``dataFormat`` as well as various available decoders.

In addition to the above Kafka types, the Redis connector supports ``hash`` type for the ``value`` field which represent data stored in the Redis hash.
Redis connector use `hgetall key` to get data.
``` json
      {
      "tableName": ...,
      "schemaName": ...,
      "value": {
        "dataFormat": "hash",
        "fields": [
          ...
        ]
      }
    }
```

the Redis connector supports ``zset`` type for the ``key`` field which represent key stored in the Redis zset. 
if and only if ``zset`` is used as key datafomart,the split is truly supported , because we can use `zrange zsetkey split.start split.end` to get keys of a split.
``` json
      {
      "tableName": ...,
      "schemaName": ...,
      "key": {
        "dataFormat": "zset",
        "name": "zsetkey", //zadd zsetkey score member
        "fields": [
            ...
        ]
      }
    }
```

Redis Connector Limitations
---------------------------
only support read operation,don't support write operation.


