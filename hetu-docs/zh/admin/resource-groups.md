
# 资源组

资源组限制资源使用，并可以对在其内部运行的查询执行排队策略，或在子组之间划分资源。一个查询属于单个资源组，并使用该组（及其祖先）的资源。除了对排队查询进行限制外，资源组耗尽资源时不会导致正在运行的查询失败；相反，新的查询将进入排队状态。资源组可以有子组，也可以接受查询，但不能同时执行两者。

资源组和关联的选择规则由可插拔的管理器配置。添加包含以下内容的`etc/resource-groups.properties`文件，使内置管理器能够读取JSON配置文件：

``` properties
resource-groups.configuration-manager=file
resource-groups.config-file=etc/resource-groups.json
```

将`resource-groups.config-file`的值修改为指向一个JSON配置文件，可以是绝对路径，也可以是相对于openLooKeng数据目录的路径。

### 其他配置

除了`etc/resource-groups.properties`中的上述属性外，还可以配置以下两个属性，这些属性与kill策略一起使用（详细信息与kill策略的部分相同）

`resource-groups.memory-margin-percent`（可选）-这是两个被认为相同的查询之间所允许的内存变化百分比。在此情况下，查询不会根据内存使用情况进行排序，而是根据查询执行进度进行排序，前提是查询进度差异大于配置差异。默认值为10%。

`resource-groups.query-progress-margin-percent`（可选）-这是两个被认为相同的查询之间所允许的查询执行进度百分比。在此情况下，查询不会根据执行进度进行排序。默认值为5%。

## 资源组属性


- `name`（必填）：群组名称。可以是模板（见下文）。

-   `maxQueued`（必填）：最大排队查询数。一旦达到此限制，新的查询将被拒绝。
-   `hardConcuritiesLimit`（必填）：最大运行查询数。
-   `softMemoryLimit`（必填）：新查询进入队列之前，该组可以使用的最大分布式内存量。可指定为集群内存的绝对值（如`1GB`）或百分比（如`10%`）。
-   `softCpuLimit`（可选）：在对最大运行查询数进行惩罚之前的时间内（参见`cpuQuotaPeriod`），该组可能使用的最大CPU时间。同时必须指定`hardCpuLimit`。
-   `hardCpuLimit`（可选）：该组在一段时间内可能使用的最大CPU时间。
-   `schedulingPolicy`（可选）：指定如何选择运行排队的查询，以及子组如何成为符合条件的查询。可以配置为以下三种策略之一。当集群开启高可用模式（多个coordinator）时，仅支持`fair`调度策略：
    - `fair`（默认）：排队的查询按先进先出的顺序处理，子组必须轮流启动新查询（如果它们有排队的话）。
    - `weighted_fair`：根据子组的`schedulingWeight`和子网并发的查询数量选择子组。子组正在运行的查询预期份额基于当前所有符合条件的子组权重计算。选择与其份额相比并发数最小的子组开始下一次查询。
    - `weighted`：按照优先级（通过`query_priority`[会话属性](../sql/set-session.md)指定）的随机选择排队的查询。按照`schedulingWeight`的比例选择子组以启动新查询。
    - `query_priority`：所有子组都必须配置`query_priority`。排队的查询将严格按照其优先级进行选择。
-   `schedulingWeight`（可选）：该子组的权重。参见上文。默认为`1`。
-   `jmxExport`（可选）：如果为true，则导出群组统计信息到JMX进行监控。默认为`false`。
-   `subGroups`（可选）：子组列表。
- `killPolicy`（可选）：当查询提交给worker后，如果总内存使用量超过**softMemoryLimit**，选择其中一种策略终止正在运行的查询。

  - `no_kill`（默认值）：不终止查询。
  - `recent_queries`：根据执行顺序的倒序进行查询终止。
  - `oldest_queries`：根据执行顺序进行查询终止。
  - `high_memory_queries`：根据内存使用量进行查询终止。具有较高内存使用量的查询将首先被终止，以便在查询终止次数最少的情况下，释放更多内存。
    作为此策略的一部分，我们尝试平衡内存使用量和完成百分比。因此，如果两个查询的内存使用量都在限制的10%以内（可通过resource-groups.memory-margin-percent配置），则进度慢（执行的百分比）的查询被终止。如果这两个查询在完成百分比方面的差异在5%以内（可通过resource-groups.query-progress-margin-percent配置），则内存使用量大的查询被终止。

  - `finish_percentage_queries`：根据查询执行百分比进行查询终止。执行百分比最小的查询将首先被终止。


## 选择器规则

-   `user`（可选）：用于匹配用户名的正则表达式。
-   `source`（可选）：用于匹配源字符串的正则表达式。
-   `queryType`（可选）：用于匹配提交的查询类型的字符串。
-   `DATA_DEFINITION`：修改/创建/删除模式/表/视图的元数据，以及管理预备语句、权限、会话和事务的查询。
- `DELETE`：`DELETE`查询。
- `DESCRIBE`：`DESCRIBE`、`DESCRIBE INPUT`、`DESCRIBE OUTPUT`以及`SHOW`查询。
    - `EXPLAIN`：`EXPLAIN`查询。
    - `INSERT`：`INSERT`和`CREATE TABLE AS`查询。
    - `SELECT`：`SELECT`查询。
-   `clientTags`（可选）：标签列表。为了能成功匹配，此列表中的每个标签都必须在客户端提供的与查询关联的标签列表中。
-   `group`（必填）：这些查询将运行的组。

选择器按顺序处理，并将使用第一个匹配的选择器。

## 全局属性


-   `cpuQuotaPeriod`（可选）：CPU配额的执行周期。

## 提供选择器属性

源名称的设置方式如下：

- CLI：使用`--source`选项。
- JDBC：在`Connection`实例上设置`ApplicationName`客户端信息属性。

客户端标签的设置方式如下：

- CLI：使用`--client-tags`选项。
- JDBC：在`Connection`实例上设置`ClientTags`客户端信息属性。

## 限制和终止查询

查询提交给worker后，可能超出内存限制，需要使用以下机制处理正在运行的查询：

- 限制查询
- 终止查询

### 限制查询

对新的split schedule进行限制，避免worker内存占用进一步增加。如果当前查询资源组的内存使用量已超过**softReservedMemory**，将不会计划进行新的拆分，除非内存使用量低于**softReservedMemory***。

建议配置**softReservedMemory**小于**softMemoryLimit**。

用户还可以选择省略**softReservedMemory**配置 从而禁用限制查询。

### 终止查询

如果查询无法被限制，并且内存使用量超过softMemoryLimit，则将根据配置的终止策略终止查询（使查询失败）。只有子组运行的查询才会被终止。

## 示例


-   在以下示例配置中，部分资源组是模板。模板允许管理员动态构建资源组树。例如，在`pipeline_${USER}`组中，`${USER}`将被拓展为提交查询的用户的名称。同时支持`${SOURCE}`拓展为提交查询的源。还可以在`source`和`user`正则表达式中使用自定义命名变量。
    
     
    
    以下四个选择器可以用来定义哪些查询在哪个资源组中运行：
    
     
    
    > - 第一个选择器匹配来自`bob`的查询，并将其放置在管理组中。
    > - 第二个选择器匹配源名称中包括`pipeline`的所有数据定义（DDL）查询，并将其放置在`global.data_definition`组中，以减少此类查询的排队时间，因为它们会被快速执行。
    > - 第三个选择器匹配源名称中包括`pipeline`的查询，并将其放置在`global.pipeline`组下动态创建的每个用户管道组中。
    > - 第四个选择器匹配来自BI工具的查询，BI工具有一个源与正则表达式`jdbc#(?.*)`匹配，并且客户端提供的标签是`hi-pri`的超集。这些查询被放置在`global.pipeline.tools`组下动态创建的子组中。动态子组将基于命名变量`toolname`创建，该命名变量从源的正则表达式中提取。假设有一个源为`jdbc#powerfulbi`，用户为`kayla`，客户端标签为`hipri`和`fast`的查询。此查询将被路由到`global.pipeline.bi-powerfulbi.kayla`资源组。
    > - 最后一个选择器是一个回收器，将所有尚未匹配的查询放入每个用户的临时组中。
    
     
    
    
    
    这些选择器共同执行以下策略：
    
     
    
    - 用户`bob`是管理员，最多可以运行50个并发查询。查询将根据用户提供的优先级运行。
    
     
    
    对于其余用户：
    
     
    
    - 并发运行的查询总数不得超过100个。
    - 最多可以运行5个源为`pipeline`的并发DDL查询。查询按先进先出顺序运行。
    - 非DDL查询将在`global.Peline`组下运行，总并发量为45，每个用户并发量为5。查询按先进先出顺序运行。
    - 对于BI工具，每个工具最多可以运行10个并发查询，每个用户最多可以运行3个并发查询。如果总需求超过10的限制，那么运行查询最少的用户将获得下一个并发槽位。这种策略在争用时保证公平。
    - 其余的查询都放在`global.adhoc.other`下的每个用户组中，每个用户组的行为类似。


[resource-groups-example.json](resource-groups-example.json)

