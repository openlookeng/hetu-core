
# Kafka连接器教程

## 介绍

适用于openLooKeng的Kafka连接器允许使用openLooKeng从Apache Kafka访问实时主题数据。本教程演示如何设置主题以及如何创建支持openLooKeng表的主题描述文件。

## 安装

本教程假定用户熟悉openLooKeng且本地安装有可用的openLooKeng（见[手动部署openLooKeng](../installation/deployment.md)）。教程将专注于设置Apache Kafka并将其与openLooKeng集成。

### 步骤1：安装Apache Kafka

下载并解压[Apache Kafka](https://kafka.apache.org/ )。

**说明**

*本教程使用Apache Kafka 0.8.1进行了测试。教程应适用于任何0.8.x版本的Apache Kafka。*

启动ZooKeeper和Kafka服务器：

``` shell
$ bin/zookeeper-server-start.sh config/zookeeper.properties
[2013-04-22 15:01:37,495] INFO Reading configuration from: config/zookeeper.properties (org.apache.zookeeper.server.quorum.QuorumPeerConfig)
...
```

``` shell
$ bin/kafka-server-start.sh config/server.properties
[2013-04-22 15:01:47,028] INFO Verifying properties (kafka.utils.VerifiableProperties)
[2013-04-22 15:01:47,051] INFO Property socket.send.buffer.bytes is overridden to 1048576 (kafka.utils.VerifiableProperties)
...
```

这将在端口`2181`上启动ZooKeeper，在端口`9092`上启动Kafka。

### 步骤2：加载数据

从Maven Central下载tpch-kafka加载器：

``` shell
$ curl -o kafka-tpch https://repo1.maven.org/maven2/de/softwareforge/kafka_tpch_0811/1.0/kafka_tpch_0811-1.0.sh
$ chmod 755 kafka-tpch
```

现在运行`kafka-tpch`程序，预加载带有tpch数据多个主题：

``` shell
$ ./kafka-tpch load --brokers localhost:9092 --prefix tpch. --tpch-type tiny
2014-07-28T17:17:07.594-0700     INFO    main    io.airlift.log.Logging    Logging to stderr
2014-07-28T17:17:07.623-0700     INFO    main    de.softwareforge.kafka.LoadCommand    Processing tables: [customer, orders, lineitem, part, partsupp, supplier, nation, region]
2014-07-28T17:17:07.981-0700     INFO    pool-1-thread-1    de.softwareforge.kafka.LoadCommand    Loading table 'customer' into topic 'tpch.customer'...
2014-07-28T17:17:07.981-0700     INFO    pool-1-thread-2    de.softwareforge.kafka.LoadCommand    Loading table 'orders' into topic 'tpch.orders'...
2014-07-28T17:17:07.981-0700     INFO    pool-1-thread-3    de.softwareforge.kafka.LoadCommand    Loading table 'lineitem' into topic 'tpch.lineitem'...
2014-07-28T17:17:07.982-0700     INFO    pool-1-thread-4    de.softwareforge.kafka.LoadCommand    Loading table 'part' into topic 'tpch.part'...
2014-07-28T17:17:07.982-0700     INFO    pool-1-thread-5    de.softwareforge.kafka.LoadCommand    Loading table 'partsupp' into topic 'tpch.partsupp'...
2014-07-28T17:17:07.982-0700     INFO    pool-1-thread-6    de.softwareforge.kafka.LoadCommand    Loading table 'supplier' into topic 'tpch.supplier'...
2014-07-28T17:17:07.982-0700     INFO    pool-1-thread-7    de.softwareforge.kafka.LoadCommand    Loading table 'nation' into topic 'tpch.nation'...
2014-07-28T17:17:07.982-0700     INFO    pool-1-thread-8    de.softwareforge.kafka.LoadCommand    Loading table 'region' into topic 'tpch.region'...
2014-07-28T17:17:10.612-0700    ERROR    pool-1-thread-8    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.region
2014-07-28T17:17:10.781-0700     INFO    pool-1-thread-8    de.softwareforge.kafka.LoadCommand    Generated 5 rows for table 'region'.
2014-07-28T17:17:10.797-0700    ERROR    pool-1-thread-3    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.lineitem
2014-07-28T17:17:10.932-0700    ERROR    pool-1-thread-1    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.customer
2014-07-28T17:17:11.068-0700    ERROR    pool-1-thread-2    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.orders
2014-07-28T17:17:11.200-0700    ERROR    pool-1-thread-6    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.supplier
2014-07-28T17:17:11.319-0700     INFO    pool-1-thread-6    de.softwareforge.kafka.LoadCommand    Generated 100 rows for table 'supplier'.
2014-07-28T17:17:11.333-0700    ERROR    pool-1-thread-4    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.part
2014-07-28T17:17:11.466-0700    ERROR    pool-1-thread-5    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.partsupp
2014-07-28T17:17:11.597-0700    ERROR    pool-1-thread-7    kafka.producer.async.DefaultEventHandler    Failed to collate messages by topic, partition due to: Failed to fetch topic metadata for topic: tpch.nation
2014-07-28T17:17:11.706-0700     INFO    pool-1-thread-7    de.softwareforge.kafka.LoadCommand    Generated 25 rows for table 'nation'.
2014-07-28T17:17:12.180-0700     INFO    pool-1-thread-1    de.softwareforge.kafka.LoadCommand    Generated 1500 rows for table 'customer'.
2014-07-28T17:17:12.251-0700     INFO    pool-1-thread-4    de.softwareforge.kafka.LoadCommand    Generated 2000 rows for table 'part'.
2014-07-28T17:17:12.905-0700     INFO    pool-1-thread-2    de.softwareforge.kafka.LoadCommand    Generated 15000 rows for table 'orders'.
2014-07-28T17:17:12.919-0700     INFO    pool-1-thread-5    de.softwareforge.kafka.LoadCommand    Generated 8000 rows for table 'partsupp'.
2014-07-28T17:17:13.877-0700     INFO    pool-1-thread-3    de.softwareforge.kafka.LoadCommand    Generated 60175 rows for table 'lineitem'.
```

现在，Kafka拥有了多个预先装载了要查询的数据的主题。

### 步骤3：使Kafka主题对openLooKeng可见

在openLooKeng安装中，为Kafka连接器添加目录属性文件`etc/catalog/kafka.properties`。该文件列出了Kafka节点和主题：

``` properties
connector.name=kafka
kafka.nodes=localhost:9092
kafka.table-names=tpch.customer,tpch.orders,tpch.lineitem,tpch.part,tpch.partsupp,tpch.supplier,tpch.nation,tpch.region
kafka.hide-internal-columns=false
```

现在启动openLooKeng：

``` shell
$ bin/launcher start
```

因为Kafka的表在配置中都有`tpch.`前缀，所以这些表都在`tpch`模式中。因为属性文件命名为`kafka.properties`，所以连接器被装入到`kafka`目录中。

启动[openLooKeng CLI](../installation/cli.md)：

``` shell
$ ./openlk-cli --catalog kafka --schema tpch
```

列出表，以验证操作是否成功：

``` sql
lk:tpch> SHOW TABLES;
  Table
----------
 customer
 lineitem
 nation
 orders
 part
 partsupp
 region
 supplier
(8 rows)
```

### 步骤4：基础数据查询

Kafka数据是非结构化的，没有元数据来描述消息的格式。无需进一步配置，Kafka连接器可以访问数据并以原始形式映射数据，但除了内置列之外没有实际列：

``` sql
lk:tpch> DESCRIBE customer;
      Column       |  Type   | Extra |                   Comment
-------------------+---------+-------+---------------------------------------------
 _partition_id     | bigint  |       | Partition Id
 _partition_offset | bigint  |       | Offset for the message within the partition
 _segment_start    | bigint  |       | Segment start offset
 _segment_end      | bigint  |       | Segment end offset
 _segment_count    | bigint  |       | Running message count per segment
 _key              | varchar |       | Key text
 _key_corrupt      | boolean |       | Key data is corrupt
 _key_length       | bigint  |       | Total number of key bytes
 _message          | varchar |       | Message text
 _message_corrupt  | boolean |       | Message data is corrupt
 _message_length   | bigint  |       | Total number of message bytes
(11 rows)

lk:tpch> SELECT count(*) FROM customer;
 _col0
-------
  1500

lk:tpch> SELECT _message FROM customer LIMIT 5;
                                                                                                                                                 _message
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 {"rowNumber":1,"customerKey":1,"name":"Customer#000000001","address":"IVhzIApeRb ot,c,E","nationKey":15,"phone":"25-989-741-2988","accountBalance":711.56,"marketSegment":"BUILDING","comment":"to the even, regular platelets. regular, ironic epitaphs nag e"}
 {"rowNumber":3,"customerKey":3,"name":"Customer#000000003","address":"MG9kdTD2WBHm","nationKey":1,"phone":"11-719-748-3364","accountBalance":7498.12,"marketSegment":"AUTOMOBILE","comment":" deposits eat slyly ironic, even instructions. express foxes detect slyly. blithel
 {"rowNumber":5,"customerKey":5,"name":"Customer#000000005","address":"KvpyuHCplrB84WgAiGV6sYpZq7Tj","nationKey":3,"phone":"13-750-942-6364","accountBalance":794.47,"marketSegment":"HOUSEHOLD","comment":"n accounts will have to unwind. foxes cajole accor"}
 {"rowNumber":7,"customerKey":7,"name":"Customer#000000007","address":"TcGe5gaZNgVePxU5kRrvXBfkasDTea","nationKey":18,"phone":"28-190-982-9759","accountBalance":9561.95,"marketSegment":"AUTOMOBILE","comment":"ainst the ironic, express theodolites. express, even pinto bean
 {"rowNumber":9,"customerKey":9,"name":"Customer#000000009","address":"xKiAFTjUsCuxfeleNqefumTrjS","nationKey":8,"phone":"18-338-906-3675","accountBalance":8324.07,"marketSegment":"FURNITURE","comment":"r theodolites according to the requests wake thinly excuses: pending
(5 rows)

lk:tpch> SELECT sum(cast(json_extract_scalar(_message, '$.accountBalance') AS double)) FROM customer LIMIT 10;
   _col0
------------
 6681865.59
(1 row)
```

Kafka中的数据可以使用openLooKeng查询，但并不是呈实际表的形式。原始数据可以通过`_message`和`_key`列获得，但不会解码为列。由于样本数据是JSON格式，因此可以使用openLooKeng内置的[json](../functions/json.md)对数据进行切片。

### 步骤5：添加主题描述文件

Kafka连接器支持主题描述文件，将原始数据转换为表格式。这些文件位于openLooKeng安装的`etc/kafka`文件夹中，必须以`.json`结尾。建议文件名与表名匹配，但不一定必须匹配。

将如下文件添加为`etc/kafka/tpch.customer.json`，并重启openLooKeng。

``` json
{
    "tableName": "customer",
    "schemaName": "tpch",
    "topicName": "tpch.customer",
    "key": {
        "dataFormat": "raw",
        "fields": [
            {
                "name": "kafka_key",
                "dataFormat": "LONG",
                "type": "BIGINT",
                "hidden": "false"
            }
        ]
    }
}
```

customer表现在有一个额外的列：`kafka_key`。

``` sql
lk:tpch> DESCRIBE customer;
      Column       |  Type   | Extra |                   Comment
-------------------+---------+-------+---------------------------------------------
 kafka_key         | bigint  |       |
 _partition_id     | bigint  |       | Partition Id
 _partition_offset | bigint  |       | Offset for the message within the partition
 _segment_start    | bigint  |       | Segment start offset
 _segment_end      | bigint  |       | Segment end offset
 _segment_count    | bigint  |       | Running message count per segment
 _key              | varchar |       | Key text
 _key_corrupt      | boolean |       | Key data is corrupt
 _key_length       | bigint  |       | Total number of key bytes
 _message          | varchar |       | Message text
 _message_corrupt  | boolean |       | Message data is corrupt
 _message_length   | bigint  |       | Total number of message bytes
(12 rows)

lk:tpch> SELECT kafka_key FROM customer ORDER BY kafka_key LIMIT 10;
 kafka_key
-----------
         0
         1
         2
         3
         4
         5
         6
         7
         8
         9
(10 rows)
```

主题定义文件将内部Kafka密钥（8字节，原始长度）映射到openLooKeng `BIGINT`列。

### 步骤6：将来自主题消息的所有值映射到列

更新`etc/kafka/tpch.customer.json`文件，为消息添加字段，并重启openLooKeng。由于消息中的字段是JSON，因此使用`json`数据格式。以下是对键和消息使用不同的数据格式的示例。

``` json
{
    "tableName": "customer",
    "schemaName": "tpch",
    "topicName": "tpch.customer",
    "key": {
        "dataFormat": "raw",
        "fields": [
            {
                "name": "kafka_key",
                "dataFormat": "LONG",
                "type": "BIGINT",
                "hidden": "false"
            }
        ]
    },
    "message": {
        "dataFormat": "json",
        "fields": [
            {
                "name": "row_number",
                "mapping": "rowNumber",
                "type": "BIGINT"
            },
            {
                "name": "customer_key",
                "mapping": "customerKey",
                "type": "BIGINT"
            },
            {
                "name": "name",
                "mapping": "name",
                "type": "VARCHAR"
            },
            {
                "name": "address",
                "mapping": "address",
                "type": "VARCHAR"
            },
            {
                "name": "nation_key",
                "mapping": "nationKey",
                "type": "BIGINT"
            },
            {
                "name": "phone",
                "mapping": "phone",
                "type": "VARCHAR"
            },
            {
                "name": "account_balance",
                "mapping": "accountBalance",
                "type": "DOUBLE"
            },
            {
                "name": "market_segment",
                "mapping": "marketSegment",
                "type": "VARCHAR"
            },
            {
                "name": "comment",
                "mapping": "comment",
                "type": "VARCHAR"
            }
        ]
    }
}
```

现在，对于消息的JSON中的所有字段，都定义了列，并且来自早期的求和查询可以直接对`account_balance`列进行操作：

``` sql
lk:tpch> DESCRIBE customer;
      Column       |  Type   | Extra |                   Comment
-------------------+---------+-------+---------------------------------------------
 kafka_key         | bigint  |       |
 row_number        | bigint  |       |
 customer_key      | bigint  |       |
 name              | varchar |       |
 address           | varchar |       |
 nation_key        | bigint  |       |
 phone             | varchar |       |
 account_balance   | double  |       |
 market_segment    | varchar |       |
 comment           | varchar |       |
 _partition_id     | bigint  |       | Partition Id
 _partition_offset | bigint  |       | Offset for the message within the partition
 _segment_start    | bigint  |       | Segment start offset
 _segment_end      | bigint  |       | Segment end offset
 _segment_count    | bigint  |       | Running message count per segment
 _key              | varchar |       | Key text
 _key_corrupt      | boolean |       | Key data is corrupt
 _key_length       | bigint  |       | Total number of key bytes
 _message          | varchar |       | Message text
 _message_corrupt  | boolean |       | Message data is corrupt
 _message_length   | bigint  |       | Total number of message bytes
(21 rows)

lk:tpch> SELECT * FROM customer LIMIT 5;
 kafka_key | row_number | customer_key |        name        |                address                | nation_key |      phone      | account_balance | market_segment |                                                      comment
-----------+------------+--------------+--------------------+---------------------------------------+------------+-----------------+-----------------+----------------+---------------------------------------------------------------------------------------------------------
         1 |          2 |            2 | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak        |         13 | 23-768-687-3665 |          121.65 | AUTOMOBILE     | l accounts. blithely ironic theodolites integrate boldly: caref
         3 |          4 |            4 | Customer#000000004 | XxVSJsLAGtn                           |          4 | 14-128-190-5944 |         2866.83 | MACHINERY      |  requests. final, regular ideas sleep final accou
         5 |          6 |            6 | Customer#000000006 | sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn  |         20 | 30-114-968-4951 |         7638.57 | AUTOMOBILE     | tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious
         7 |          8 |            8 | Customer#000000008 | I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5 |         17 | 27-147-574-9335 |         6819.74 | BUILDING       | among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly alon
         9 |         10 |           10 | Customer#000000010 | 6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2    |          5 | 15-741-346-9870 |         2753.54 | HOUSEHOLD      | es regular deposits haggle. fur
(5 rows)

lk:tpch> SELECT sum(account_balance) FROM customer LIMIT 10;
   _col0
------------
 6681865.59
(1 row)
```

现在，`customer`主题消息的所有字段都作为openLooKeng表列可用。

### 步骤7：使用实时数据

openLooKeng可以在实时数据到达时从Kafka中查询实时数据。为了模拟一个实时数据推送，本教程将实时推文的推送设置到Kafka中。

#### 设置实时Twitter推送

- 下载twistr工具

``` shell
$ curl -o twistr https://repo1.maven.org/maven2/de/softwareforge/twistr_kafka_0811/1.2/twistr_kafka_0811-1.2.sh
$ chmod 755 twistr
```

- 在<https://dev.twitter.com/>创建开发者账号，并设置访问和消费token。
- 创建`twistr.properties`文件，将访问密钥和消费者密钥放入其中：

``` properties
twistr.access-token-key=...
twistr.access-token-secret=...
twistr.consumer-key=...
twistr.consumer-secret=...
twistr.kafka.brokers=localhost:9092
```

#### 在openLooKeng上创建tweets表

在`etc/catalog/kafka.properties`文件中添加tweets表：

``` properties
connector.name=kafka
kafka.nodes=localhost:9092
kafka.table-names=tpch.customer,tpch.orders,tpch.lineitem,tpch.part,tpch.partsupp,tpch.supplier,tpch.nation,tpch.region,tweets
kafka.hide-internal-columns=false
```

将Twitter推送的主题定义文件添加为`etc/kafka/tweets.json`：

``` json
{
    "tableName": "tweets",
    "topicName": "twitter_feed",
    "dataFormat": "json",
    "key": {
        "dataFormat": "raw",
        "fields": [
            {
                "name": "kafka_key",
                "dataFormat": "LONG",
                "type": "BIGINT",
                "hidden": "false"
            }
        ]
    },
    "message": {
        "dataFormat":"json",
        "fields": [
            {
                "name": "text",
                "mapping": "text",
                "type": "VARCHAR"
            },
            {
                "name": "user_name",
                "mapping": "user/screen_name",
                "type": "VARCHAR"
            },
            {
                "name": "lang",
                "mapping": "lang",
                "type": "VARCHAR"
            },
            {
                "name": "created_at",
                "mapping": "created_at",
                "type": "TIMESTAMP",
                "dataFormat": "rfc2822"
            },
            {
                "name": "favorite_count",
                "mapping": "favorite_count",
                "type": "BIGINT"
            },
            {
                "name": "retweet_count",
                "mapping": "retweet_count",
                "type": "BIGINT"
            },
            {
                "name": "favorited",
                "mapping": "favorited",
                    "type": "BOOLEAN"
            },
            {
                "name": "id",
                "mapping": "id_str",
                "type": "VARCHAR"
            },
            {
                "name": "in_reply_to_screen_name",
                "mapping": "in_reply_to_screen_name",
                "type": "VARCHAR"
            },
            {
                "name": "place_name",
                "mapping": "place/full_name",
                "type": "VARCHAR"
            }
        ]
    }
}
```

由于此表没有显式的模式名称，因此将把它放入`default`模式中。

#### 推送实时数据

启动twistr工具：

``` shell
$ java -Dness.config.location=file:$(pwd) -Dness.config=twistr -jar ./twistr
```

`twistr`连接Twitter API，并将“sample tweet”推送到名为`twitter_feed`的Kafka主题。

现在对实时数据运行查询：

``` shell
$ ./openlk-cli --catalog kafka --schema default

lk:default> SELECT count(*) FROM tweets;
 _col0
-------
  4467
(1 row)

lk:default> SELECT count(*) FROM tweets;
 _col0
-------
  4517
(1 row)

lk:default> SELECT count(*) FROM tweets;
 _col0
-------
  4572
(1 row)

lk:default> SELECT kafka_key, user_name, lang, created_at FROM tweets LIMIT 10;
     kafka_key      |    user_name    | lang |       created_at
--------------------+-----------------+------+-------------------------
 494227746231685121 | burncaniff      | en   | 2014-07-29 14:07:31.000
 494227746214535169 | gu8tn           | ja   | 2014-07-29 14:07:31.000
 494227746219126785 | pequitamedicen  | es   | 2014-07-29 14:07:31.000
 494227746201931777 | josnyS          | ht   | 2014-07-29 14:07:31.000
 494227746219110401 | Cafe510         | en   | 2014-07-29 14:07:31.000
 494227746210332673 | Da_JuanAnd_Only | en   | 2014-07-29 14:07:31.000
 494227746193956865 | Smile_Kidrauhl6 | pt   | 2014-07-29 14:07:31.000
 494227750426017793 | CashforeverCD   | en   | 2014-07-29 14:07:32.000
 494227750396653569 | FilmArsivimiz   | tr   | 2014-07-29 14:07:32.000
 494227750388256769 | jmolas          | es   | 2014-07-29 14:07:32.000
(10 rows)
```

现在有一个实时推送到Kafka，可以使用openLooKeng查询。

### 结语：时间戳

在上一步中设置的tweets推送在每个tweet中都包含一个RFC 2822格式的时间戳作为`created_at`属性。

``` sql
lk:default> SELECT DISTINCT json_extract_scalar(_message, '$.created_at')) AS raw_date
             -> FROM tweets LIMIT 5;
            raw_date
--------------------------------
 Tue Jul 29 21:07:31 +0000 2014
 Tue Jul 29 21:07:32 +0000 2014
 Tue Jul 29 21:07:33 +0000 2014
 Tue Jul 29 21:07:34 +0000 2014
 Tue Jul 29 21:07:35 +0000 2014
(5 rows)
```

tweets表的主题定义文件包含使用`rfc2822`转换器转换到时间戳的映射：

``` json
...
{
    "name": "created_at",
    "mapping": "created_at",
    "type": "TIMESTAMP",
    "dataFormat": "rfc2822"
},
...
```

这允许将原始数据映射到openLooKeng时间戳列：

``` sql
lk:default> SELECT created_at, raw_date FROM (
             ->   SELECT created_at, json_extract_scalar(_message, '$.created_at') AS raw_date
             ->   FROM tweets)
             -> GROUP BY 1, 2 LIMIT 5;
       created_at        |            raw_date
-------------------------+--------------------------------
 2014-07-29 14:07:20.000 | Tue Jul 29 21:07:20 +0000 2014
 2014-07-29 14:07:21.000 | Tue Jul 29 21:07:21 +0000 2014
 2014-07-29 14:07:22.000 | Tue Jul 29 21:07:22 +0000 2014
 2014-07-29 14:07:23.000 | Tue Jul 29 21:07:23 +0000 2014
 2014-07-29 14:07:24.000 | Tue Jul 29 21:07:24 +0000 2014
(5 rows)
```

Kafka连接器包含用于ISO8601、RFC 2822文本格式以及使用自epoch时间以来的秒或毫秒的数的时间戳的转换器。还有一个通用的、基于文本的格式化程序，它使用Joda-Time格式字符串来解析文本列。