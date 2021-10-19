External Function Registration and Push Down
==============

Introduction
------------
The connector can register `external function` into openLooKeng. In Jdbc connector, openLooKeng can push them down to data source which support to execute those functions.

Function Registration in Connector
----------------------------------
The connector can register `external function` into openLooKeng.

The user can find the example code about how to register `external function` through connector in `presto-mysql/src/main/java/io.prestosql/plugin/mysql/`.
It is an example implement in mysql connector.
There are two steps to register external functions through a connector.
1. Developer need to extend the interface 'ExternalFunctionHub' and implement the related method:
```JAVA
public interface ExternalFunctionHub
{
    Set<ExternalFunctionInfo> getExternalFunctions();

    RoutineCharacteristics.Language getExternalFunctionLanguage();

    CatalogSchemaName getExternalFunctionCatalogSchemaName();
}
```

The `getExternalFunctions` method return the external function set.
About how to implement it, we supply example code in the `MysqlExternalFunctionHub.java`.
In this example, about implementation of `ExternalFunctionHub#getExternalFunctions`, 
we build `ExternalFunctionInfo` static instances and register them to return set.
Of course, you can build you code to load the set of `ExternalFunctionInfo` instances, for example, load an external file and serialize it to `ExternalFunctionInfo` instances.
In this example, we only supply a general-purpose framework.

Only the following types which define in the `io.prestosql.spi.type.StandardTypes` are supported to declared in external functions.

| Supported type                                      |
| ------------------------------------------------------------ |
|  StandardTypes.TINYINT |
|  StandardTypes.SMALLINT|
|  StandardTypes.INTEGER |
|  StandardTypes.BIGINT |
|  StandardTypes.DECIMAL |
|  StandardTypes.REAL |
|  StandardTypes.DOUBLE |
|  StandardTypes.BOOLEAN |
|  StandardTypes.CHAR |
|  StandardTypes.VARCHAR |
|  StandardTypes.VARBINARY |
|  StandardTypes.DATE |
|  StandardTypes.TIME |
|  StandardTypes.TIMESTAMP |
|  StandardTypes.TIME_WITH_TIME_ZONE |
|  StandardTypes.TIMESTAMP_WITH_TIME_ZONE |


2. After you implement the `ExternalFunctionHub` interface,
you only need to register `ExternalFunctionHub` instance into the Connector. You can find example code in `MySqlClientModule.java` and `MysqlJdbcClient.java`.


Configuration of External Function Namespace
--------------------------------------------
After you finish the registration code,
 you must config a property named `connector.externalfunction.namespace` in the connector's catalog property file.
 It loads the `catalog.schema` for the external functions the connector register to the `function namespace manager`.
For example, you can set the property in the `etc/catalog/mysql.properties` as following:

```Properties
connector.externalfunction.namespace=mysqlfun.default
```

External Function Push Down
---------------------------
After the external functions register to the `function namespace manager`, Jdbc Connector can push external function down to the data source, and take result back.
Before we build the code to support `external function` push down in the Jdbc connector, we must first support `Query Push Down`.
This feature can easily implement by extends the class in the package `presto-base-jdbc/src/main/java/io.prestosql/plugin/jdbc/optimization`.
Now openLooKeng support `Query Push Down` in the datacenter、hana、oracle、mysql and greenplum connector.

Base on the `Query Push Down`, we need 3 steps to support external function push down in a Jdbc Connector.
1. Extends the `ApplyRemoteFunctionPushDown.java`
Firstly, you need to implement the method `ApplyRemoteFunctionPushDown#rewriteRemoteFunction` in `ApplyRemoteFunctionPushDown.java`.
This method rewrites the external function register in openLookeng to a SQL string written in the SQL format of the data source.
You can refer to the example implementation in `presto-mysql/src/main/java/io.prestosql/plugin/mysql/optimization/function/MySqlApplyRemoteFunctionPushDown.java`.

2. Extends `BaseJdbcRowExpressionConverter.java`
You should override the method `BaseJdbcRowExpressionConverter#visitCall` in the `presto-base-jdbc/src/main/java/io.prestosql/plugin/jdbc/optimization/BaseJdbcRowExpressionConverter.java`.
You need to identify the external function in this method and call the `ApplyRemoteFunctionPushDown#rewriteRemoteFunction` override in the step one to rewrite the external function register in openLookeng to a SQL string written in the SQL format of the data source.
You can refer to an example code in the `presto-mysql/src/main/java/io.prestosql/plugin/mysql/optimization/MySqlRowExpressionConverter.java`.

3. Configuration of `external function` namespace the connector support
After steps 1 and 2, you need to config the `catalog.schema` the connector support to push down in the connector catalog property file.
For example, in the file `etc/catalog/mysql.properties` should contain the content following:
```Properties
jdbc.pushdown.remotenamespace=mysqlfun.default
```
A connector instance can declare that it support push down external functions which come from multiple function namespaces: 
```Properties
jdbc.pushdown.remotenamespace=mysqlfun1.default|mysqlfun2.default|mysqlfun3.default
#declare that connector can support to push down external function register in mysqlfun1.default, mysqlfun2.default and mysqlfun3.default.
```
Now you can write SQL which contain external functions to run in openLooKeng.
For example:
```SQL
select mysqlfun.default.format(col1, 2) from double_table;
```
The external function `mysqlfun.default.format` presents in the SQL will be rewritten into a suitable SQL grammar and push down to the data source, for example:
```SQL
SELECT format(col1,2)  FROM double_table
```
