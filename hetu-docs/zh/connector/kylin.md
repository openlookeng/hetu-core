
# Kylin连接器

Kylin连接器允许在外部Kylin数据库中查询表。这可用于在Kylin和Hive等不同系统之间或在两个不同的Kylin实例之间联接数据。

## 配置

要配置Kylin连接器，在`etc/catalog`中创建一个目录属性文件，例如`Kylin.properties`，将Kylin连接器挂载为`kylin`目录。使用以下内容创建文件，并根据设置替换连接属性：

``` properties
connector.name=kylin
connection-url=jdbc:kylin://example.net/project
connection-user=root
connection-password=secret
connector-planoptimizer-rule-blacklist=io.prestosql.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy
```

其中connector-planoptimizer-rule-blacklist属性是对kylin特殊配置的，默认值就为io.prestosql.sql.planner.iterative.rule.SingleDistinctAggregationToGroupBy

### 多个Kylin服务器

可以根据需要创建任意多的目录，因此，如果有额外的Kylin服务器，只需添加另一个不同名称的属性文件到`etc/catalog`中（确保它以`.properties`结尾）。例如，如果将属性文件命名为`sales.properties`，openLooKeng将使用配置的连接器创建一个名为`sales`的目录。

## 查询Kylin

可以访问`web`数据库中`clicks`的表：

    SELECT * FROM kylin.web.clicks;

如果对目录属性文件使用不同的名称，请使用该目录名称，而不要使用上述示例中的`kylin`。

## Kylin连接器限制

暂不支持以下SQL语句：

[DELETE](../sql/insert.md)、../sql/update.md)、../sql/delete.md)、[GRANT](../sql/grant.md)、[REVOKE](../sql/revoke.md)、[SHOW GRANTS](../sql/show-grants.md)、[SHOW ROLES](../sql/show-roles.md)、[SHOW ROLE GRANTS](../sql/show-role-grants.md)