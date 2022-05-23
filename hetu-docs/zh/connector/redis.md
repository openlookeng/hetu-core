Redis 连接器
====================
概述
--------
此连接器允许在openlookeng中，将redis中一个kv键值对映射成表的一行数据

**说明**

*kv键值对只能在Reids中映射为string或hash类型。keys可以存储为一个zset,然后keys可以被分割成多个片*

*支持 Redis 2.8.0 或更高版本*


配置
-------------
要配置Redis连接器，创建具有以下内容的目录属性文件etc/catalog/redis.properties，并适当替换以下属性：
``` properties
connector.name=redis
redis.table-names=schema1.table1,schema1.table2
redis.nodes=host1:port
```
### 多个 Redis Servers
可以根据需要创建任意多的目录，因此，如果有额外的redis server，只需添加另一个不同的名称的属性文件到etc/catalog中（确保它以.properties结尾）。例如，如果将属性文件命名为sales.properties，openLooKeng将使用配置的连接器创建一个名为sales的目录。

配置属性
------------------------
配置属性包括：

| 属性名称                                                       | 说明                                                                          |
|:-----------------------------------------------------------|:----------------------------------------------------------------------------|
| `redis.table-names`                                        | catalog 提供的所有表的列表                                                           |
| `redis.default-schema`                                     | 表的默认schema名 (默认`default`)                                                   |
| `redis.nodes`                                              | Redis server的节点列表                                                           |
| `redis.connect-timeout`                                    | 连接Redis server的超时时间 (ms) (默认 2000)                                          |
| `redis.scan-count`                                         | 每轮scan获得的key的数量 (默认 100)                                                    |
| `redis.key-prefix-schema-table`                            | Redis keys 是否有 schema-name:table-name的前缀   (默认  false)                      |
| `redis.key-delimiter`                                      | 如果`redis.key-prefix-schema-table`被启用，那么schema-name和table-name的分隔符为 (默认 `:`) |
| `redis.table-description-dir`                              | 存放表定义json文件的相对地址 (默认 `etc/redis/`)                                          |
| `redis.hide-internal-columns`                              | 内部列是否在元数据中隐藏(默认 true)                                                       |
| `redis.database-index`                                     | Redis database 的索引  (默认 0)                                                  |
| `redis.password`                                           | Redis server 密码 (默认 null)                                                   |
| `redis.table-description-interval`                         | flush表定义文件的时间间隔（默认不会flush,意味着Plugin被加载后,表定义一直留存在内存中,不再主动读取json文件）           | 

内部列
----------------

| 列名        | 类型    | 说明                                                  |
|:-------------------| :------ |:----------------------------------------------------|
| `_key`             | VARCHAR  | Redis key.                                          |
| `_value`           | VARCHAR   | 与key相对应的值                                           |
| `_key_length`   | BIGINT  | key  的字节大小                                          |
| `_key_corrupt`     | BOOLEAN  | 如果解码器无法解码此行的key，则为true。当为 true 时，从键映射的数据列应被视为无效。    |
| `_value_corrupt`   | BOOLEAN | 如果解码器无法解码此行的value，则为true。当为 true 时，从该值映射的数据列应被视为无效。 |



表定义文件
----------------------
对于openLooKeng，每个kv键值对 必须被映射到列中，以便允许对数据查询。这很像kafka connector,所以你可以参考 kafka连接器教程


表定义文件由一个表的JSON定义组成。文件名可以任意，但必须以.json结尾。

以nation.json为例
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
在redis,相应的有这样的数据
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
我们可以使用redis connector从redis中获取数据，（redis_key没有显示，这是因为我们设置了"hidden": "true" ）
```shell
lk> select * from redis.tpch.nation;
 nationkey |      name      | regionkey |                                                      comment                                                       
-----------+----------------+-----------+--------------------------------------------------------------------------------------------------------------------
         3 | CANADA         |         1 | eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold              
         9 | INDONESIA      |         2 |  slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull 
        19 | ROMANIA        |         3 | ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account    
         2 | BRAZIL         |         1 | y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special   
```
**说明**

*如果属性 `redis.key-prefix-schema-table` 是false (默认是false),那么当前所有key的都会被视作的nation表的key,不会发生匹配过滤*

有关各种可用解码器的描述，请参考kafka连接器教程

除了kafka支持的类型，Redis connector对value字段支持hash类型
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

Redis connector 支持``zset`` 作为``key``在Redis中存储类型。
当且仅当``zset`` 作为key的存储格式时，split切片功能才能被真正支持，因为我们可以使用`zrange zsetkey split.start split.end`来得到一个切片的keys
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

Redis 连接器的局限性
---------------------------
只支持读操作，不支持写操作.


