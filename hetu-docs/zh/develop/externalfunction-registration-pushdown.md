# 外部函数注册和下推


## 介绍

openLooKeng系统可以在Jdbc Connector中注册外部函数到`function-namespace-manager`中，并且可以通过Jdbc Connector下推到数据源执行。

## 在Jdbc Connector中注册外部函数

用户可以在`presto-mysql/src/main/java/io.prestosql/plugin/mysql/optimization/function`中找到如何在Jdbc Connector向`function-namespace-manager`注册外部函数的代码。
用户只要扩展实现下面这个抽象函数类中的 `getExternalFunctions` 方法(例如`MysqlExternalFunctionHub.java`)：
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
此方法返回Connector需要向`function-namespace-manager`注册的函数信息。
关于如何实现`JdbcExternalFunctionHub#getExternalFunctions`，我们在`MysqlExternalFunctionHub.java`中提供了一个简单明了的例子，
我们通过静态类实例注册的形式来实现`ExternalFunctionInfo`的实例集合的构造。当然你也可以自己定制代码，通过从外部读取文件等形式来构造`ExternalFunctionInfo`的实例集合，
也就是你只需要注册和返回一个`ExternalFunctionInfo`的实例集合即可。在这里我们仅提供基本的通用框架。

实现JdbcExternalFunctionHub之后，你只需要将其通过Jdbc Connector的plugin实现类(例如`MySqlPlugin.java`)注册到JdbcConnectionFactory中。

## 配置外部函数注册的命名空间

在完成上述的注册过程之后，你就可以在Connector的Catalog文件中配置你所注册的外部函数需要写入的`function-namespace-manager`的`catalog.schema`的函数命名空间。
例如在`etc/catalog/mysql.properties`配置`catalog.schema`函数命名空间为`example.default`：

```Properties
connector.externalfunction.namespace=example.default
```

## 外部函数下推

外部函数注册完成后，当前openLooKeng可以通过Jdbc Connector下推到支持执行这些外部函数的数据源中执行，进而取回外部函数处理数据的结果。
一个Jdbc Connector在适配外部函数下推功能之前，首先需要适配Query Push Down功能。当前Query Push Down功能
可以很容易通过继承使用`presto-base-jdbc/src/main/java/io.prestosql/plugin/jdbc/optimization`提供的基础类来实现，在这里就不再赘述。
openLooKeng已经支持了包括datacenter、hana、oracle、mysql、greenplum等connector的Query Push Down功能。

基于Query Push Down功能的基础上，要支持外部函数下推，需要以下步骤。
1. 扩展实现`ApplyRemoteFunctionPushDown.java`
首先你需要实现`ApplyRemoteFunctionPushDown.java`中的`ApplyRemoteFunctionPushDown#rewriteRemoteFunction`方法。此方法的作用是将openLooKeng中注册的外部函数，
重写为符合数据源SQL语法的字符串。你可以直接参考`presto-mysql/src/main/java/io.prestosql/plugin/mysql/optimization/function/MySqlApplyRemoteFunctionPushDown.java`中的实现。

2. 扩展实现`BaseJdbcRowExpressionConverter`
扩展重载`presto-base-jdbc/src/main/java/io.prestosql/plugin/jdbc/optimization/BaseJdbcRowExpressionConverter.java`中的`BaseJdbcRowExpressionConverter#visitCall`方法。
你需要在此方法中处理识别外部函数，并调用`ApplyRemoteFunctionPushDown#rewriteRemoteFunction`方法得到符合数据源SQL语法的字符串返回。
具体细节可以参考`presto-mysql/src/main/java/io.prestosql/plugin/mysql/optimization/MySqlRowExpressionConverter.java`。

3.配置当前Connector支持下推的函数命名空间
在上述代码开发完成后，你需要将当前Connector支持的函数命名空间配置在Connector的Catalog文件中。例如在`etc/catalog/mysql.properties`中配置：
```Properties
jdbc.pushdown.remotenamespace=example.default
```
一个Connector实例可以声明自己支持多个函数命名空间中的函数，在`jdbc.pushdown.remotenamespace`配置项中使用'|'分割既可。例如：
```Properties
jdbc.pushdown.remotenamespace=example1.default|example2.default|example3.default|
#表示当前Connector实例同时支持example1.default、example2.default、example3.default三个函数命名空间最终的函数下推到当前连接的数据源中执行。
```

在外部函数下推功能适配完成后，你就可以直接在输入openLooKeng的SQL 语句中直接使用外部函数处理数据源的数据。系统会将外部函数下推到Connector连接的数据源执行。
例如SQL 语句：
```SQL
select example.default.format(col1, 2) from double_table;
```
外部函数 `example.default.format`会下推到数据源执行。下推语句为符合数据源SQL 语法的SQL语句，例如可能为：
```SQL
SELECT format(col1,2)  FROM double_table
```
