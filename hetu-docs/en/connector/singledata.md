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

- SingleData connector only supports ShardingSphere 5.3.0 now

## TidRange Mode

### Overview

OpenGauss data is stored in HEAP PAGE by row. Each row of data has a corresponding ctid (line number). The tidrangescan plugin of openGauss can use the range of ctid for query. The tidRange mode uses this plugin to implement parrallel data analysis.

To obtain the tidrangescan plugin, please visit: [Plugin](https://gitee.com/opengauss/Plugin)

### Connection Configuration

To configure the singleData Connector for tidRange mode, create a catalog properties file `etc/catalog` named, for example, `tidrange.properties`. Create the file with the following contents, replacing the connection properties as appropriate for your setup:
```properties
connector.name=singledata
singledata.mode=TID_RANGE
connection-url=jdbc:opengauss://master-host:port/database;jdbc:opengauss://slave-host:port/database
connection-user=user
connection-password=password
```
- connection-url can be configured multiple JDBC connection addresses. The addresses are separated by `;`.

### Optional Configuration

| Property Name                           | Description                                                                                                                                                                          | Default |
|-----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| tidrange.max-connection-count-per-node  | Maximum number of connections on each openGauss node. The minimum value is 1. The sum of the values of all workers cannot be greater than the value of max_connections of openGauss. | 100     |
| tidrange.max-table-split-count-per-node | Maximum table split count of per openGauss node, The minimum value is 1                                                                                                              | 50      |
| tidrange.connection-timeout             | Maximum waiting time for open connection. The unit is ms. If the value is 0, the connection never times out. If the value is not 0, the minimum value is 250ms.                      | 0       |
| tidrange.max-lifetime                   | Maximum connection lifetime, in milliseconds. The minimum value is 30000ms.The default value is 30 minutes                                                                           | 1800000 |

## Limitations
- SingleData Connector only support select statement now, statement such as INSERT/UPDATE/DELETE that modify data or data structures are not supported.
- The maximum decimal precision of openLookeng is 38. If the precision of Decimal or Numeric is greater than 38 in openGauss, it will be not supported.
- The openGauss version required 3.0.0 or later
