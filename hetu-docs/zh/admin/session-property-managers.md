
# 会话属性管理器

管理员可以添加会话属性来控制其工作负载子集的行为。这些属性是默认属性，可以被用户重写（如果被授权）。会话属性可用于控制资源使用、启用或禁用特性以及更改查询特性。会话属性管理器可插拔。

添加具有如下内容的`etc/session-property-config.properties`文件使内置管理器可以读取JSON配置文件：

``` properties
session-property-config.configuration-manager=file
session-property-manager.config-file=etc/session-property-config.json
```

将`session-property-manager.config-file`的值修改为指向一个json配置文件，可以是绝对路径，也可以是相对于openLooKeng数据目录的相对路径。

此配置文件由匹配规则列表和默认情况下应应用的会话属性列表组成，每条匹配规则指定查询必须满足的条件列表。所有匹配规则都有助于构造会话属性列表。规则按指定的顺序应用。文件中较晚指定的规则将重写以前遇到的属性的值。

## 匹配规则

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
- `group`（可选）:用于匹配查询路由到的资源组的完全限定名的正则表达式。
- `sessionProperties`：与字符串键和值的映射。每个条目是系统或目录属性名和相应的值。值必须指定为字符串，而不管实际的数据类型是什么。

## 示例

考虑以下一组要求：

- 在`global`资源组下运行的所有查询的执行时间限制必须为8小时。
- 所有交互查询都路由到`global.interactive`组下的子组，并且执行时间限制为1小时（比对`global`的约束更严格）。
- 所有ETL查询（标记为‘etl’）都被路由到`global.pipeline`组下的子组，并且必须用某些属性配置以控制编写器行为。

这些要求可以用以下规则来表达：

``` json
[
  {
    "group": "global.*",
    "sessionProperties": {
      "query_max_execution_time": "8h",
    }
  },
  {
    "group": "global.interactive.*",
    "sessionProperties": {
      "query_max_execution_time": "1h"
    }
  },
  {
    "group": "global.pipeline.*",
    "clientTags": ["etl"],
    "sessionProperties": {
      "scale_writers": "true",
      "writer_min_size": "1GB"
    }
  }
]
```