# SingleData Connector

The singleData connector supplements the OLAP capability for the openGauss, enhances the capability of data analysis.

The singleData mode can be ShardingSphere or tidRange

## ShardingSphere Mode

### Overview

Apache ShardingSphere is a distributed database ecosystem. It can convert any database into a distributed database. SingleData Connector can use ShardingSphere to shard openGauss data to improve analysis efficiency.

For more information, please visit: [ShardingSphere official website](https://shardingsphere.apache.org/)

### Configuration

To configure the singleData Connector for ShardingSphere mode, create a catalog properties file `etc/catalog` named, for example, `shardingsphere.properties`. Create the file with the following contents, replacing the connection properties as appropriate for your setup:

- Basic Configuration

``` properties
connector.name=singledata
singledata.mode=SHARDING_SPHERE
shardingsphere.database-name=sharding_db
shardingsphere.type=zookeeper
shardingsphere.namespace=governance_ds
shardingsphere.server-list=localhost:2181
```

- Configuration Properties

| Property Name                | Description                                                                                                      | required |
|------------------------------|------------------------------------------------------------------------------------------------------------------|----------|
| shardingsphere.database-name | Name of the connected shardingSphere database                                                                    | Yes      |
| shardingsphere.type          | Persistent repository type of the registration center.Zookeeper and etcd are supported. Zookeeper is recommended | Yes      |
| shardingsphere.namespace     | NameSpace of the registry center                                                                                 | Yes      |
| shardingsphere.server-lists  | Connection address list of the registry center                                                                   | Yes      |

- Optional configuration when the persistent repository type is zookeeper

| Property Name                                           | Description                                             | Default |
|---------------------------------------------------------|---------------------------------------------------------|---------|
| shardingsphere.zookeeper.retry-interval-milliseconds    | Retry interval after connection failure in milliseconds | 500     |
| shardingsphere.zookeeper.max-retries                    | Maximum retry connections                               | 3       |
| shardingsphere.zookeeper.time-to-live-seconds           | Temporary node lifetime seconds                         | 60      |
| shardingsphere.zookeeper.operation-timeout-milliseconds | Operation timeout milliseconds                          | 500     |
| shardingsphere.zookeeper.digest                         | Token                                                   | ""      |

- Optional configuration when the persistent repository type is etcd

| Property Name                            | Description                     | Default |
|------------------------------------------|---------------------------------|---------|
| shardingsphere.etcd.time-to-live-seconds | Temporary node lifetime seconds | 30      |
| shardingsphere.etcd.connection-timeout   | Connection timeout seconds      | 3       |

****Note:****

- SingleData connector only supports ShardingSphere 5.2.0 now

## TidRange Mode

### Overview

OpenGauss data is stored in HEAP PAGE by row. Each row of data has a corresponding ctid (line number). The tidrangescan plugin of openGauss can use the range of ctid for query. The tidRange mode uses this plugin to implement parrallel data analysis.

To obtain the tidrangescan plugin, please visit: [Plugin](https://gitee.com/opengauss/Plugin)

### Connection Configuration

To configure the singleData Connector for tidRange mode, create a catalog properties file `etc/catalog` named, for example, `tidrange.properties`. Create the file with the following contents, replacing the connection properties as appropriate for your setup:
```properties
connection.name=singledata
connection.mode=TID_RANGE
connection-url=jdbc:opengauss://master-host:port/database;jdbc:opengauss://slave-host:port/database
connection-user=user
connection-password=password
```

- connection-url can be configured multiple JDBC connection addresses. The addresses are separated by `;`. During query, each segment randomly selects a connection address for connection.
- For other connection configurations, see the openGauss connector

### Split Configuration

| Property Name               | Description                                                                                                                                                        | Required | Default |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------|
| tidrange.max-split-count    | Max count of split, that is, the maximum number of JDBC connections. The value must be less than or equal to the value of max_connections in openGauss             | No       | 100     |
| tidrange.page-size          | Page size of the openGauss. Ensure that the value is same as the value of block_size of the openGauss. Otherwise, the query result may be incorrect                | No       | 8kB     |
| tidrange.default-split-size | Default size of each split. When the data size is small, the singleData connector fragments the data bases on this configuration. The value ranges from 1MB to 1GB | No       | 32MB    |

****NOTE:****

- TidRange mode needs to be used with tidrangescan plugin of the openGauss, without tidrangescan plugin, the singledata connector can be used for query, but the performance deteriorates greatly.
- TidRange will be not enabled when an index exists in the table

## Limitations
- SingleData Connector only support select statement now, statement such as INSERT/UPDATE/DELETE that modify data or data structures are not supported.
- The maximum decimal precision of openLookeng is 38. If the precision of Decimal or Numeric is greater than 38 in openGauss, it will be not supported.
- The openGauss version required 3.0.0 or later
