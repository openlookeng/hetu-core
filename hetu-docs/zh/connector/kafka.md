
# Kafka连接器

## 概述

此连接器允许在openLooKeng中将Apache Kafka主题用作表。每条消息在openLooKeng中显示为一行。

主题可以是实时的：当数据到达时，行将出现，当段被删除时，行将消失。如果在单个查询中多次访问同一个表（例如，执行自联接），这可能会导致奇怪的行为。

**说明**

*Kafka代理最低支持版本为0.10.0。*

## 配置

要配置Kafka连接器，创建具有以下内容的目录属性文件`etc/catalog/kafka.properties`，并适当替换以下属性：

``` properties
connector.name=kafka
kafka.table-names=table1,table2
kafka.nodes=host1:port,host2:port
```

### 多Kafka集群

可以根据需要创建任意多的目录，因此，如果有额外的Kafka集群，只需添加另一个不同的名称的属性文件到`etc/catalog`中（确保它以`.properties`结尾）。例如，如果将属性文件命名为`sales.properties`，openLooKeng将使用配置的连接器创建一个名为`sales`的目录。

## 配置属性

配置属性包括：

| 属性名称| 说明|
|:----------|:----------|
| `kafka.table-names`| 目录提供的所有表列表|
| `kafka.default-schema`| 表的默认模式名|
| `kafka.nodes`| Kafka集群节点列表|
| `kafka.connect-timeout`| 连接Kafka集群超时|
| `kafka.buffer-size`| Kafka读缓冲区大小|
| `kafka.table-description-dir`| 包含主题描述文件的目录|
| `kafka.hide-internal-columns`| 控制内部列是否是表模式的一部分|

### `kafka.table-names`

此目录提供的所有表的逗号分隔列表。表名可以是非限定的（简单名称），并将被放入默认模式（见下文）中，或者用模式名称（`<schema-name>.<table-name>`）限定。

对于这里定义的每个表，都可能存在一个表描述文件（见下文）。如果没有表描述文件，则使用表名作为Kafka的主题名称，且数据列不映射到表。该表仍将包含所有内部列（见下文）。

此属性是必需的；没有默认值，并且必须至少定义一个表。

### `kafka.default-schema`

定义将包含没有定义限定模式名称的所有表的模式。

此属性是可选的；默认值为`default`。

### `kafka.nodes`

Kafka数据节点的`hostname:port`对的逗号分隔列表。

此属性是必需的；没有默认值，并且必须至少定义一个表。

**说明**

openLooKeng必须仍然能够连接到群集的所有节点，即使这里只指定了子集，因为段文件可能只位于特定的节点上。

### `kafka.connect-timeout`

连接数据节点超时。繁忙的Kafka集群在接受连接之前可能要花费一些时间；当看到由于超时而导致的查询失败时，增加该值是一种很好的策略。

此属性是可选的；默认值为10秒（`10s`）。

### `kafka.buffer-size`

从Kafka读取数据的内部数据缓冲区大小。数据缓冲区必须至少能够容纳一条消息，理想情况下可以容纳多条消息。每个工作节点和数据节点分配一个数据缓冲区。

此属性是可选的；默认值为`64kb`。

### `kafka.table-description-dir`

在openLooKeng部署中引用一个文件夹，其中包含一个或多个JSON文件（必须以`.json`结尾），其中包含表描述文件。

此属性是可选的；默认值为`etc/kafka`。

### `kafka.hide-internal-columns`

除了在表描述文件中定义数据列外，连接器还为每个表维护许多附加列。如果这些列是隐藏的，它们仍然可以在查询中使用，但是不会显示在`DESCRIBE <table-name>`或`SELECT *`中。

此属性是可选的；默认值为`true`。

## 内部列

对于每个已定义的表，连接器维护以下列：

| 列名| 类型| 说明|
|:----------|:----------|:----------|
| `_partition_id`| BIGINT| 包含该行的Kafka分区ID。|
| `_partition_offset`| BIGINT| 此行在Kafka分区内的偏移量。|
| `_segment_start`| BIGINT| 包含该行的段（包括该段）中最小的偏移量。这个偏移量是分区特定的。|
| `_segment_end`| BIGINT| 包含该行的段（不包括该段）中最大的偏移量。这个偏移量是分区特定的。这与下一个段（如果存在）的`_segment_start`的值相同。|
| `_segment_count`| BIGINT| 段内当前行的运行计数。对于未压缩的主题，`_segment_start + _segment_count`等于`_partition_offset`。|
| `_message_corrupt`| BOOLEAN| 如果解码器无法解码此行的消息，则为true。如果为true，则应将消息映射的数据列视为无效。|
| `_message`| VARCHAR| 作为UTF-8编码的字符串的消息字节。这只对文本主题有用。|
| `_message_length`| BIGINT| 消息字节数。|
| `_key_corrupt`| BOOLEAN| 如果解码器无法解码此行的键，则为true。如果为true，则应将该键映射的数据列视为无效。|
| `_key`| VARCHAR| 作为UTF-8编码的字符串的键字节。这只对文本键有用。|
| `_key_length`| BIGINT| 键字节数。|

对于没有表定义文件的表，`_key_corrupt`列和`_message_corrupt`列将始终为`false`。

## 表定义文件

Kafka仅以字节消息的形式维护主题，并让生产者和消费者定义如何解释消息。对于openLooKeng，必须将这些数据映射到列中，以便允许对数据进行查询。

**说明**

对于包含JSON数据的文本主题，完全可以使用openLooKeng `/functions/json`来解析包含映射到UTF-8字符串中的字节的`_message`列，而不用任何表定义文件。但是这相当麻烦，并且使得编写SQL查询变得很困难。

表定义文件由一个表的JSON定义组成。文件名可以任意，但必须以`.json`结尾。

``` json
{
    "tableName": ...,
    "schemaName": ...,
    "topicName": ...,
    "key": {
        "dataFormat": ...,
        "fields": [
            ...
        ]
    },
    "message": {
        "dataFormat": ...,
        "fields": [
            ...
       ]
    }
}
```

| 字段| 是否必填| 类型| 说明|
|:----------|:----------|:----------|:----------|
| `tableName`| 必填| string| 该文件定义的openLooKeng表名。|
| `schemaName`| 可选| string| 将包含表的模式。如果省略，则使用默认模式名称。|
| `topicName`| 必填| string| 映射的Kafka主题。|
| `key`| 可选| JSON对象| 映射到消息键的数据列的字段定义。|
| `message`| 可选| JSON对象| 映射到消息本身的数据列的字段定义。|

## Kafka中的键和消息

从Kafka 0.8版本开始，每个主题中的每条消息都可以有一个可选的键。表定义文件包含键和消息的节，用于将数据映射到表列。

表定义中的`key`字段和`message`字段均为必须包含两个字段的JSON对象：

| 字段| 是否必填| 类型| 说明|
|:----------|:----------|:----------|:----------|
| `dataFormat`| 必填| string| 选择该组字段的解码器。|
| `fields`| 必填| JSON数组| 字段定义列表。每个字段定义在openLooKeng表中创建一个新列。|

每个字段定义都是一个JSON对象：

``` json
{
    "name": ...,
    "type": ...,
    "dataFormat": ...,
    "mapping": ...,
    "formatHint": ...,
    "hidden": ...,
    "comment": ...
}
```

| 字段| 是否必填| 类型| 说明|
|:----------|:----------|:----------|:----------|
| `name`| 必填| string| openLooKeng表中的列名。|
| `type`| 必填| string| 列的openLooKeng类型。|
| `dataFormat`| 可选| string| 选择该字段的列解码器。默认使用此行数据格式和列类型的默认解码器。|
| `dataSchema`| 可选| string| Avro模式所在的路径或URL。仅用于Avro解码器。|
| `mapping`| 可选| string| 列的映射信息。这是解码器特定的，见下文。|
| `formatHint`| 可选| string| 设置列解码器的列特定格式提示。|
| `hidden`| 可选| boolean| 将列对`DESCRIBE`和`SELECT *`隐藏。默认为`false`。|
| `comment`| 可选| string| 添加列注释，该注释通过`DESCRIBE`显示。|

键或消息的字段说明不受限制。

## 行解码

对于键和消息，使用解码器将消息和键数据映射到表列。

Kafka连接器包含以下的解码器：

- `raw` - 不解释Kafka消息，将原始消息字节范围映射到表列
- `csv` - Kafka消息被解释为逗号分隔消息，字段映射到表列
- `json` - Kafka消息解释为JSON，JSON字段映射到表列
- `avro` - Kafka消息按照Avro模式解析，Avro字段映射到表列

**说明**

如果表没有定义文件，则使用`dummy`解码器，该解码器不暴露任何列。

### `raw`解码器

Raw解码器支持从Kafka消息或键中读取原始（基于字节）值并将其转换为openLooKeng列。

对于字段，支持如下属性：

- `dataFormat`-选择转换数据类型的宽度
- `type` - openLooKeng数据类型（支持的数据类型列表见下表）
- `mapping` - `<start>[:<end>]`；要转换的字节的开始和结束位置（可选）

`dataFormat`属性选择转换的字节数。如果不填，则假定为`BYTE`。所有值都有符号。

支持的值为：

- `BYTE` - 1字节
- `SHORT` - 2字节（大端序）
- `INT` - 4字节（大端序）
- `LONG` - 8字节（大端序）
- `FLOAT` - 4字节（IEEE 754格式）
- `DOUBLE` - 8字节（IEEE 754格式）

`type`属性定义值映射到的openLooKeng数据类型。

根据分配给列的openLooKeng类型，可以使用不同的dataFormat值：

| openLooKeng数据类型| 允许`dataFormat`值|
|:----------|:----------|
| `BIGINT`| `BYTE`、`SHORT`、`INT`、`LONG`|
| `INTEGER`| `BYTE`、`SHORT`、`INT`|
| `SMALLINT`| `BYTE`、`SHORT`|
| `TINYINT`| `BYTE`|
| `DOUBLE`| `DOUBLE`、`FLOAT`|
| `BOOLEAN`| `BYTE`、`SHORT`、`INT`、`LONG`|
| `VARCHAR` / `VARCHAR(x)`| `BYTE`|

`mapping`属性指定用于解码的键或消息中的字节范围。可以是1个或2个数字，中间用冒号隔开（`<start>[:<end>]`）。

如果只给出起始位置：

> - 对于固定宽度类型，该列将对指定的`dateFormat`使用适当字节数（见上文）。
> - 当`VARCHAR`值被解码时，从起始位置到消息结尾的所有字节将被使用。

如果给出开始和结束位置，则：

> - 对于固定宽度类型，大小必须等于指定`dataFormat`所使用的字节数。
> - 对于`VARCHAR`，起始（包括）和结束（不包括）之间的所有字节都将被使用。

如果未指定`mapping`属性，则等效于将起始位置设置为0，而将结束位置设置为未定义。

数值数据类型（`BIGINT`、`INTEGER`、`SMALLINT`、`TINYINT`、`DOUBLE`）的解码方案非常简单。从输入消息中读取字节序列并根据以下任一条件进行解码：

> - 大端序编码（integer类型）
> - IEEE 754格式（用于`DOUBLE`）。

`dataFormat`所隐含的已解码字节序列的长度。对于`VARCHAR`数据类型，字节序列根据UTF-8编码进行解释。

### `csv`解码器

CSV解码器将代表消息或键的字节转换为UTF-8编码的字符串，然后将结果解释为CSV（逗号分隔值）行。

对于字段，必须定义`type`和`mapping`属性：

- `type` - openLooKeng数据类型（支持的数据类型列表见下表）
- `mapping` - CSV记录中字段的索引

`dataFormat`和`formatHint`不支持，必须省略。下表列出了支持的openLooKeng类型，可用于`type`和解码方案：

| openLooKeng数据类型| 解码规则|
|:----------|:----------|
| `BIGINT` `INTEGER` `SMALLINT` `TINYINT`| 使用Java `Long.parseLong()`解码|
| `DOUBLE`| 使用Java `Double.parseDouble()`解码|
| `BOOLEAN`| “true”字符序列映射到`true`；其他字符序列映射到`false`|
| `VARCHAR` / `VARCHAR(x)`| 原样使用|

### `json`解码器

JSON解码器根据`4627`将代表消息或键的字节转换为JSON。请注意，消息或键*必须*转换为JSON对象，而不是数组或简单类型。

对于字段，支持如下属性：

- `type` - 列的openLooKeng类型。
- `dataFormat` - 用于列的字段解码器。
- `mapping` - 以斜杠分隔的字段名列表，用于从JSON对象中选择字段
- `formatHint` - 仅限`custom-date-time`，详见下文

JSON解码器支持多个字段解码器，`_default`用于标准表列和许多基于日期和时间的类型的解码器。

下表列出了可如`type`中使用的openLooKeng数据类型和可通过`dataFormat`属性指定的匹配字段解码器。

| openLooKeng数据类型| 允许`dataFormat`值|
|:----------|:----------|
| `BIGINT` `INTEGER` `SMALLINT` `TINYINT` `DOUBLE` `BOOLEAN` `VARCHAR` `VARCHAR(x)`| 默认字段解码器（省略`dataFormat`属性）|
| `TIMESTAMP` `TIMESTAMP WITH TIME ZONE` `TIME` `TIME WITH TIME ZONE`| `custom-date-time`、`iso8601`、`rfc2822`、`milliseconds-since-epoch`、`seconds-since-epoch`|
| `DATE`| `custom-date-time`、`iso8601`、`rfc2822`|

### 默认字段译码器

这是标准的字段解码器，支持所有的openLooKeng物理数据类型。通过JSON转换规则，字段值将被强制转换为boolean值、long值、double值或string值。对于非基于日期/时间的列，应使用此解码器。

### 日期和时间解码器

如果需要将JSON对象中的值转换为openLooKeng `DATE`、`TIME`、`TIME WITH TIME ZONE`、`TIMESTAMP`或`TIMESTAMP WITH TIME ZONE`列，则需要通过字段定义的`dataFormat`属性选择特定的解码器。

- `iso8601` - 基于文本，将文本字段解析为ISO8601时间戳。

- `rfc2822` - 基于文本，将文本字段解析为`2822`时间戳。

- `custom-date-time` - 基于文本，根据通过`formatHint`属性指定的Joda格式模式解析一个文本字段。格式模式应符合<https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html>。

- `milliseconds-since-epoch` - 基于数字，将文本或数字解释为自epoch时间以来的毫秒数。

- `seconds-since-epoch` - 基于数字，将文本或数字解释为自epoch时间以来的毫秒数。

对于`TIMESTAMP WITH TIME ZONE`和`TIME WITH TIME ZONE`数据类型，如果解码值中存在时区信息，则在openLooKeng值中使用时区。否则，结果时区将被设置为`UTC`。

### `avro`解码器

Avro解码器根据模式转换表示Avro格式的消息或键的字节。消息必须嵌入Avro模式。openLooKeng不支持无模式Avro解码。

对于键/消息，使用`avro`解码器时，必须定义`dataSchema`。这应该指向需要解码的消息的有效Avro模式文件的位置。此位置可以是远程Web服务器（例如：`dataSchema: 'http://example.org/schema/avro_data.avsc'`）或本地文件系统（例如：`dataSchema: '/usr/local/schema/avro_data.avsc'`）。如果无法从openLooKeng协调节点访问此位置，解码器将失败。

对于字段，支持如下属性：

- `name` - openLooKeng表中的列名。
- `type` - 列的openLooKeng类型。
- `mapping` - 以斜杠分隔的字段名列表，用于从Avro模式中选择字段如果`mapping`中指定的字段在原始Avro模式中不存在，则读取操作将返回NULL。

下表列出了支持的openLooKeng类型，可在`type`中用于等价的Avro字段类型。

| openLooKeng数据类型| 允许的Avro数据类型|
|:----------|:----------|
| `BIGINT`| `INT`、`LONG`|
| `DOUBLE`| `DOUBLE`、`FLOAT`|
| `BOOLEAN`| `BOOLEAN`|
| `VARCHAR` / `VARCHAR(x)`| `STRING`|
| `VARBINARY`| `FIXED`、`BYTES`|
| `ARRAY`| `ARRAY`|
| `MAP`| `MAP`|

#### Avro模式演进

Avro解码器支持向后兼容的模式演进特性。通过向后兼容性，就可以使用较新的模式读取用较旧的模式创建的Avro数据。Avro模式中的任何更改也必须反映在openLooKeng的主题定义文件中。Avro模式中新增/重命名的字段*必须*有默认值。

Schema的演进行为如下：

- 新模式中增加的列：当表使用新模式时，使用旧模式创建的数据将产生默认值。
- 新模式中移除的列：使用旧模式创建的数据将不再输出已移除列的数据。
- 列在新的模式中被重命名：这等价于移除列并添加新列，当表使用新模式时，使用旧模式创建的数据将产生默认值。
- 更改新模式中的列类型：如果Avro支持该类型强制，那么就会发生转换。不兼容的类型将引发错误。