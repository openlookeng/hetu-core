

# Tableau的Web连接器

Tableau的openLooKeng网络连接器允许用户在Tableau上运行对openLooKeng的查询。该连接器实现[Tableau Web连接器API](https://community.tableau.com/community/developers/web-data-connectors)中的函数。

Tableau在创建新的web数据源时，会询问Web连接器的URL。使用以下URL，`example.net:8080`替换成openLooKeng协调节点的主机名和端口号（默认端口为`8080`）：

```
http://example.net:8080/tableau/presto-connector.html
```

当Tableau第一次加载openLooKeng Web连接器时，将呈现一个HTML表单。在这个表单中，你需要填写例如用户名、要查询的目录和架构、数据源名称、要设置的会话参数以及最终要运行的SQL查询等详细信息。点击`Submit`后，查询将提交给openLooKeng协调节点，Tableau将逐页从协调节点检索的结果中创建摘录。在Tableau摘录完查询的结果之后，你可以使用此摘录进一步分析Tableau。

**说明**

*使用openLooKeng Web连接器，你只能创建Tableau摘录，因为Web连接器API目前不支持实时模式。*

*Web连接器API只支持openLooKeng中可用的数据类型的一个子集。具体而言，Tableau Web连接器API目前支持以下Tableau数据类型：`bool`、`date`、`datetime`、`float`*、*`int`和`string`。openLooKeng `boolean`和`date`类型将分别转换为Tableau客户端上的Tableau数据类型`bool`和`date`。任何其他Presto类型，如`array`、`map`、`row`*、*`double`、`bigint`等，将转换为Tableau `string`，因为它们未映射到任何Tableau类型。*