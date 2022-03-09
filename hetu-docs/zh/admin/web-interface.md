
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

## 通用属性

### `hetu.queryeditor-ui.allow-insecure-over-http`

> -   **类型:** `boolean`
> -   **允许值** `true`, `false`
> -   **默认值:** `false`
>
> 默认情况下，基于HTTP的非安全环境禁用WEB UI。可以通过配置`etc/config.properties`文件的`hetu.queryeditor-ui.allow-insecure-over-http`属性启用(例子: hetu.queryeditor-ui.allow-insecure-over-http=true)。

### `hetu.queryeditor-ui.execution-timeout`

> -   **类型：** `duration`
> -   **默认值：** `100 DAYS`
>
> UI执行超时默认设置为100天。可以通过配置`etc/config.properties`文件中的`hetu.queryeditor-ui.execution-timeout`属性修改。

### `hetu.queryeditor-ui.max-result-count`

> - **类型：** `int`
> - **默认值：** `1000`
>
> UI最大结果计数默认设置为1000。可以通过配置`etc/config.properties`文件中的`hetu.queryeditor-ui.max-result-count`属性修改。

### `hetu.queryeditor-ui.max-result-size-mb`

>- **类型：** `size`
>- **默认值：** `1GB`
>
>UI最大结果大小默认设置为1 GB。可以通过配置`etc/config.properties`文件中的`hetu.queryeditor-ui.max-result-size-mb`属性修改。

### `hetu.queryeditor-ui.session-timeout`

> -   **类型：** `duration`
> -   **默认值：** `1 DAYS`
>
> UI会话超时默认设置为1天。可以通过配置`etc/config.properties`文件中的`hetu.queryeditor-ui.session-timeout`属性修改。