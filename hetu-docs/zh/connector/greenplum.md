Greenplum Connector
====================

Greenplum Connector 可以用来在一个外部的Greenplum数据库进行数据查询、创建表格等。
还可以用来作跨数据源的数据join处理，不需要额外的数据迁移操作，例如hive、Greenplum数据源之间的数据join操作，或者两个不同的Greenplum数据源之间的数据join操作。
Greenplum Connector是基于PostgreSQL connector开发的。同时我们在Greenplum connector中添加了Query Push Down特性。

配置项
-------------

Greenplum connector的基础配置和PostgreSQL connector是一样的。
比如你可以通过在`etc/catalog`中创建一个名为 `greenplum.properties` 的文件来在系统中加载一个Greenplum connector的实例。
这个文件中包含如下配置，你可以使用正确的配置值来替换下面配置项中的示意值。

``` properties
connector.name=greenplum
connection-url=jdbc:postgresql://example.net:5432/database
connection-user=root
connection-password=secret
```

### Table修改

我们可以在`greenplum.properties`中设置`greenplum.allow-modify-table`来声明是否允许Greenplum connector修改数据源SQL表格。
如果 `greenplum.allow-modify-table` 被设置成 `false`, create/rename/add column/rename column/drop column等操作都不能执行。
默认设置为`true`。

``` properties
greenplum.allow-modify-table=true
```

### 在Greenplum connector中开启Query Push Down特性

Query Push Down特性能够把本来在openLooKeng中执行的filter, project 或者其他的一些 sql operators下推到Greenplum数据源执行，
这样可以减少openLooKeng和数据源之间的数据传输量。我们在`greenplum.properties`中配置`jdbc.pushdown-enabled`配置项来启动或者关闭这个特性。
如果`jdbc.pushdown-enabled`设置成 `false`, Query Push Down特性被关闭。
如果`jdbc.pushdown-enabled`设置成 `true`, Query Push Down特性被打开。 Query Push Down特性默认打开。
例如此配置项可以写成如下所示：

``` properties
jdbc.pushdown-enabled=false
```

- Query Push Down特性的模式选择
我们可以通过在`greenplum.properties`中配置`jdbc.pushdown-module`配置项来选择下推模式。
默认来说，我们系统使用的模式是`BASE_PUSHDOWN`. 你也可以设置成`FULL_PUSHDOWN`。
两个模式的不同之处在于：`FULL_PUSHDOWN`: 所有Query Push Down支持的算子都下推。
`BASE_PUSHDOWN`: 只下推部分算子, 包括： filter, aggregation, limit, topN and project等。
例如此配置项可以写成如下所示：

``` properties
jdbc.pushdown-module=FULL_PUSHDOWN
```

 其他
-------------
启动connector实例后，关于如何通过Greenplum Connector的实例查询数据源数据，请参考PostgreSQL connector。
 