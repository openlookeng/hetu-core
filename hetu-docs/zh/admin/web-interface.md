
# Web界面

openLooKeng提供了一个用于监视和管理查询的Web界面。Web界面可以在openLooKeng协调器上通过HTTP协议访问，使用协调器`config_properties`中指定的HTTP端口号。

主页有一个查询列表，其中包含诸如唯一查询ID、查询文本、查询状态、完成百分比、用户名和该查询的来源等信息。当前运行的查询位于页面的顶部，紧随其后的是最近完成或失败的查询。

可能的查询状态如下：

- `QUEUED`--查询已被接受，等待执行。
- `PLANNING`--查询正在规划中。
- `STARTING`--查询执行开始。
- `RUNNING`--查询中至少有一个正在运行的任务。
- `BLOCKED`--查询阻塞，等待资源（缓冲区空间、内存、分片等）。
- `FINISHING`--查询正在结束（例如：提交自动提交查询）。
- `FINISHED`--查询已经执行完成，所有输出已经消耗。
- `FAILED`--查询执行失败。

`BLOCKED`状态是正常的，但如果该状态持续，应进行调查。该状态有很多可能的原因：内存或分片不足、磁盘或网络I/O瓶颈、数据倾斜（所有数据都流向几个工作节点）、缺乏并行度（只有几个工作节点可用）或给定阶段之后的查询的计算开销大。另外，如果客户端处理数据的速度不够快，查询可能处于`BLOCKED`状态（常见于“SELECT \*”查询）。

有关查询的详细信息，请单击查询ID链接。查询详细信息页有一个摘要部分、查询的各个阶段的图形表示和任务列表。可以点击每个任务ID以获得有关该任务的更多信息。

摘要部分有一个按钮，用于终止当前正在运行的查询。在摘要部分有两个可视化：任务执行和时间线。通过单击JSON链接，可以获得包含有关查询的信息和统计信息的完整JSON文档。这些可视化和其他统计信息可用于分析查询所花费的时间。

## 连接器属性文件设置

Web界面支持通过连接器属性文件设置的方式在界面拓展和添加新的连接器，连接器属性文件可以参考项目提供的模板`presto-main/etc/connector_properties.json.template`进行设置。

系统默认使用`etc/connector_properties.json`作为Web界面连接器属性文件路径，可以通过设置`etc/config.properties`中的`hetu.queryeditor-ui.server.connector-properties-json`属性修改文件路径。

Web连接器配置文件支持配置的属性如下：

| 属性名称 | 是否必选 | 描述 |
|----------|----------|----------|
| `connectorWithProperties` | 是 | 连接器详细配置参数配置 |
| `user` | 是 | 建立连接器的用户 |
| `uuid` | 是 | 连接器唯一标识 |
| `docLink` | 是 | 连接器官方介绍文档链接 |
| `configLink` | 是 | 连接器属性配置文档链接 |

其中，`connectorWithProperties`支持配置的属性如下：

| 属性名称| 是否必选 | 描述 | 默认值 |
|----------|----------|----------|----------|
| `connectorName`| 是 | 连接器名 | |
| `connectorLabel`| 是 | 连接器描述 | |
| `propertiesEnabled` | 否 | 是否启用连接器属性配置面板，启用界面才支持配置连接器属性设置功能 | false |
| `catalogConfigFilesEnabled` | 否 | 是否启用当前连接器的配置文件上传功能 | false |
| `globalConfigFilesEnabled` | 否 | 是否启用系统配置文件（系统配置文件的配置项所有连接器共享）上传功能 | false |
| `properties` | 是 | 连接器属性配置项 | |

其中，`properties`支持配置的属性如下：

| 属性名称 | 是否必选 | 描述 | 默认值 |
|----------|----------|----------|----------|
| `name`| 是 | 属性名 | |
| `value`| 是 | 属性值 | |
| `description` | 是 | 属性描述 | |
| `required` | 否 | 该属性是否必填 | false |
| `readOnly` | 否 | 是否开启只读模式，启用的话在界面不支持修改属性值 | false |
| `type` | 是 | 配置项数据类型 | |

示例，连接器属性文件中配置DataCenter连接器属性：

``` json
[
  {
    "connectorWithProperties": {
      "connectorName": "dc",
      "connectorLabel": "DataCenter: Query data in remote OpenLooKeng data center",
      "propertiesEnabled": true,
      "catalogConfigFilesEnabled": false,
      "globalConfigFilesEnabled": false,
      "properties": [
        {
          "name": "connection-url",
          "value": "http://localhost:8080",
          "description": "The connection URL of remote OpenLooKeng data center",
          "required" : true,
          "type" : "string"
        },
        {
          "name": "connection-user",
          "value": "lk",
          "description": "User to connect to remote data center",
          "required" : true,
          "readOnly": true,
          "type": "string"
        }
      ]
    }, 
    "user": "lk",
    "uuid": "ff35a9cf-8cfc-4ff5-be2f-a6389c144f9e",
    "docLink": "https://openlookeng.io/docs/docs/connector/datacenter.html",
    "configLink": "https://openlookeng.io/docs/docs/connector/datacenter.html#configuration"
  },
  ......
]
```
