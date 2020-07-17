
# 资源组

资源组限制资源使用，并且可以对在资源组内运行的查询执行队列策略，或者在子组之间划分资源。一个查询属于单个资源组，并消耗该组（及其祖先）的资源。除了对排队查询的限制之外，当资源组耗尽资源时不会导致正在运行的查询失败。相反，新的查询会进入排队状态。资源组可以有子组，也可以接受查询，但不能两者都进行。

资源组和关联的选择规则由可插拔的管理器配置。添加具有如下内容的`etc/resource-groups.properties`文件使内置管理器可以读取JSON配置文件：

``` properties
resource-groups.configuration-manager=file
resource-groups.config-file=etc/resource-groups.json
```

将`resource-groups.config-file`的值修改为指向一个json配置文件，可以是绝对路径，也可以是相对于openLooKeng数据目录的相对路径。

## 资源组属性

- `name`（必填）：群组名称。可以是模板（见下文）。
- `maxQueued`（必填）：最大排队查询数。一旦达到此限制，新的查询将被拒绝。
- `hardConcurrencyLimit`（必填）：最大运行查询数。
- `softMemoryLimit`（必填）：新查询进入队列之前，该组可以使用的最大分布式内存量。可指明为集群内存的绝对值（即`1GB`）或百分比（即`10%`）。
- `softCpuLimit`（可选）：在对最大运行查询数进行惩罚之前的时间内（参见`cpuQuotaPeriod`）该组可能使用的最大CPU时间。必须还指定`hardCpuLimit`。
- `hardCpuLimit`（可选）：该组在一段时间内可能使用的最大CPU时间。
- `schedulingPolicy`（可选）：指定如何选择排队的查询来运行，以及子组如何成为符合条件的查询。可以是以下三个值之一：
  - `fair`（默认）：排队的查询将按先进先出的顺序处理，子组必须轮流启动新的查询（如果它们有排队的话）。
  - `weighted_fair`：根据子组的`schedulingWeight`和子组的并发数选择子组。子组正在运行的查询的预期份额基于当前所有符合条件的子组的权重计算。选择与其份额相比并发度最小的子组开始下一次查询。
- `weighted`：按其优先级（通过`query_priority`[会话属性](../sql/set-session.md)指定）的比例随机选择排队的查询。选择子组以按其`schedulingWeight`的比例启动新查询。
  - `query_priority`：所有子组都必须配置`query_priority`。排队的查询将严格按照其优先级进行选择。
- `schedulingWeight`（可选）：该子组的权重。参见上文。默认为`1`。
- `jmxExport`（可选）：如果为true，则导出群组统计信息到JMX进行监控。默认为`false`。
- `subGroups`（可选）：子群组列表。

## 选择器规则

- `user`（可选）：用于匹配用户名的正则表达式。
- `source`（可选）：用于匹配源字符串的正则表达式。
- `queryType`（可选）：用于匹配提交的查询类型的字符串：
- `DATA_DEFINITION`：更改/创建/删除模式/表/视图的元数据，以及管理预备语句、权限、会话和事务的查询。
  - `DELETE`：`DELETE`查询。
- `DESCRIBE`：`DESCRIBE`、`DESCRIBE INPUT`、`DESCRIBE OUTPUT`以及`SHOW`查询。
  - `EXPLAIN`：`EXPLAIN`查询。
  - `INSERT`：`INSERT`和`CREATE TABLE AS`查询。
  - `SELECT`：`SELECT`查询。
- `clientTags`（可选）：标签列表。要匹配，此列表中的每个标记都必须在客户端提供的与查询关联的标记列表中。
- `group`（required）：这些查询将运行在哪个组中。

选择器按顺序处理，并将使用第一个匹配的选择器。

## 全局属性

- `cpuQuotaPeriod`（可选）：CPU配额的执行周期。

## 提供选择器属性

源名称的设置方式如下：

- CLI：使用`--source`选项。
- JDBC：在`Connection`实例上设置 `ApplicationName`客户端信息属性。

客户端标签的设置方式如下：

- CLI：使用`--client-tags`选项。
- JDBC：在`Connection`实例上设置 `ClientTags`客户端信息属性。

## 示例

- 在下面的配置示例中，有几个资源组，其中部分是模板。模板允许管理员动态构建资源组树。例如，在`pipeline_${USER}`组中，`${USER}`将展开为提交查询的用户的名称。同样支持`${SOURCE}`，其将展开为提交查询的源。你也可以在`source`和`user`正则表达式中使用自定义命名变量。
  
  有四个选择器定义哪些查询在哪个资源组中运行：
  
  > - 第一个选择器将匹配来自`bob`的查询并将其置于管理组中。
  > - 第二个选择器匹配来自包括`pipeline`的源名称的所有数据定义（DDL）查询并将其放入`global.data_definition`组中。这可以帮助减少此类查询的排队时间，因为它们本应很快。
  > - 第三个选择器将匹配来自包含`pipeline`的源名称的查询，并将它们放在`global.pipeline`组下动态创建的每个用户管道组中。
  > - 第四个选择器匹配来自BI工具的查询，BI工具有一个匹配正则表达式`jdbc#(?.*)`的源，并且客户端提供的标记是`hi-pri`的超集。这些查询被放置在`global.pipeline.tools`组下的动态创建的子组中。动态子组将基于命名变量`toolname`创建，该命名变量从源的正则表达式的中提取。考虑源为`jdbc#powerfulbi`、用户为`kayla`、客户端标记为`hipri`和`fast`的查询。此查询将被路由到`global.pipeline.bi-powerfulbi.kayla`资源组。
  > - 最后一个选择器是一个回收器，将所有尚未匹配的查询放入到每个用户特定的组中。
  
  这些选择器共同执行以下策略：
  
  - 用户`bob`是管理员用户，可以运行最多50个并发查询。查询将根据用户提供的优先级运行。
  
  对于其余用户：
  
  - 同时运行的查询总数不能超过100个。
  - 最多可以运行5个源为`pipeline`的并发DDL查询。查询按先进先出顺序运行。
  - 非DDL查询将在`global.pipeline`组下运行，总并发量为45，每用户并发量为5。查询按先进先出顺序运行。
  - 对于BI工具，每个工具最多可以运行10个并发查询，每个用户最多可以运行3个并发查询。如果总需求超过10的限制，那么运行查询最少的用户将获得下一个并发槽位。这种策略在争用时保证公平。
  - 所有其余的查询都放在`global.adhoc.other`下行为类似的每个用户组中。

[resource-groups-example.json](resource-groups-example.json)