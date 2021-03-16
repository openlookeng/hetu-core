External Function Registration and Push Down
==============

Introduction
------------
The Jdbc connector extends from `presto-base-jdbc` can register `external function` to openLooKeng,
and push them down to data source which support to execute those functions.

Function Registration in the Jdbc Connector
---------------------------
The user can find the example code about how to register `external function` through Jdbc Connector in the `presto-mysql/src/main/java/io.prestosql/plugin/mysql/optimization/function`.
The only one thing that developer need to do is to extends the abstract class 'JdbcExternalFunctionHub' and implement the `getExternalFunctions` method:
```JAVA
public abstract class JdbcExternalFunctionHub
        implements ExternalFunctionHub
{
    public Set<ExternalFunctionInfo> getExternalFunctions()
    {
        return emptySet();
    }

    @Override
    public final RoutineCharacteristics.Language getExternalFunctionLanguage()
    {
        return RoutineCharacteristics.Language.JDBC;
    }
}
```
The `getExternalFunctions` method return the external function set which the connector want to register to the `function-namespace-manager`.
About how to implement the method, we supply example code in the `MysqlExternalFunctionHub.java`. We build `ExternalFunctionInfo` static instances and register them to return set.
Of course you can build you own code to load the set of `ExternalFunctionInfo` instances, for example, load an external file and format it to `ExternalFunctionInfo` instances，
In the example, we only supply a general-purpose framework.

After you implement the `getExternalFunctions` method,
you only need to register `JdbcExternalFunctionHub` instance in the JdbcPlugin(for example, `MySqlPlugin.java`) to the `JdbcConnectionFactory`.


Configuration of External Function Namespace
--------------------------------------------
After you finish the registration code finish the connector,
 you must config a `catalog.schema` for the external functions the connector register to the `function namespace manager` in the connector's catalog property file.
For example, you can set the property in the `etc/catalog/mysql.properties` as `example.default`:

```Properties
connector.externalfunction.namespace=example.default
```

External Function Push Down
---------------------------
After the external functions register to the `function namespace manager`, Jdbc Connector can push external function down to the data source, and take result back.
Before we build the code to support External Function Push Down in the Jdbc connector, we must first support `Query Push Down`.
This feature can be easily implement by extends the class in the package `presto-base-jdbc/src/main/java/io.prestosql/plugin/jdbc/optimization`.
Now openLooKeng support `Query Push Down` in the datacenter、hana、oracle、mysql and greenplum connector.

Base on the `Query Push Down`, we need 3 step to support external function push down in a Jdbc Connector.
1. Extends the `ApplyRemoteFunctionPushDown.java`
Firstly, you need to implement the method `ApplyRemoteFunctionPushDown#rewriteRemoteFunction` in `ApplyRemoteFunctionPushDown.java`.
This method rewrite the external function register in openLookeng to a SQL string written in the SQL format of the data source.
You can refer to the example implementation in `presto-mysql/src/main/java/io.prestosql/plugin/mysql/optimization/function/MySqlApplyRemoteFunctionPushDown.java`.

2. Extends `BaseJdbcRowExpressionConverter.java`
You should override the method `BaseJdbcRowExpressionConverter#visitCall` in the `presto-base-jdbc/src/main/java/io.prestosql/plugin/jdbc/optimization/BaseJdbcRowExpressionConverter.java`.
You need to identify the external function in this method and call the `ApplyRemoteFunctionPushDown#rewriteRemoteFunction` override in the step one to rewrite the external function register in openLookeng to a SQL string written in the SQL format of the data source.
You can refer to an example code in the `presto-mysql/src/main/java/io.prestosql/plugin/mysql/optimization/MySqlRowExpressionConverter.java`.

3. Configuration of External function namesapce the Connector support
After steps 1 and 2, you need to config the `catalog.schema` the connector support to push down in the connector catalog property file.
For example, in the file `etc/catalog/mysql.properties` should contain the content following:
```Properties
jdbc.pushdown.remotenamespace=example.default
```
A connector instance can declare that it support push down external functions which come from multiple function namespaces: 
```Properties
jdbc.pushdown.remotenamespace=example1.default|example2.default|example3.default|
#declare that Connector can support to push down external function register in example1.default, example2.default and example3.default.
```
Now you can write SQL which contain external functions to run in openLooKeng.
For example:
```SQL
select example.default.format(col1, 2) from double_table;
```
The external function present `example.default.format` in the SQL will be rewrite into a suitable SQL grammar and push down to the data source, for example:
```SQL
SELECT format(col1,2)  FROM double_table
```
