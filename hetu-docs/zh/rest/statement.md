
语句资源
==================

- POST /v1/statement

query query

:   需要执行的SQL查询

reqheader X-Presto-User


:   代表(optional)执行语句的用户

reqheader X-Presto-Source

:   查询的来源

reqheader X-Presto-Catalog

:   执行查询基于的目录

reqheader X-Presto-Schema

:   执行查询的模式

提交语句给openLooKeng执行。openLooKeng客户端代表用户根据指定的目录和架构进行查询。当使用openLooKeng命令行进行查询时，则调用openLooKeng协调节点的语句资源。

对语句资源的请求就是和标准X-Presto-Catalog、X-Presto-Source、X-Presto-Schema和X-Presto-User头域一起作为post执行的SQL查询。

语句资源的响应包含一个查询标识符，该标识符可用于收集有关查询的详细信息。初始响应还包括有关为执行此查询而在openLooKeng worker上创建的阶段的信息。每个查询都有一个根阶段，并且根阶段被赋予一个\"0\"的阶段标识符，如下面的响应示例所示。

这个根阶段聚合了在openLooKeng worker上运行的其他阶段的响应，并通过openLooKeng协调节点将它们传递给客户端。
当一个客户端收到这个POST的响应时，它将包含一个\"nextUri\"属性，这个属性指示客户端查询这个地址以获得查询的更多结果。

**请求样例**：

> ``` http
> POST /v1/statement HTTP/1.1
>
> Host: localhost:8001 X-Presto-Catalog: jmx X-Presto-Source: presto-cli
> X-Presto-Schema: jmx User-Agent: StatementClient/0.55-SNAPSHOT
> X-Presto-User: tobrie1 Content-Length: 41
>
> select name from "java.lang:type=runtime"
> ```

**响应样例**：

> ``` http
> HTTP/1.1 200 OK
>
> Content-Type: application/json X-Content-Type-Options: nosniff
> Transfer-Encoding: chunked
>
> {
>
>     "id":"20140108_110629_00011_dk5x2",
>     "infoUri":"<http://localhost:8001/v1/query/20140108_110629_00011_dk5x2>",
>     "partialCancelUri":"<http://192.168.1.1:8080/v1/stage/20140108_110629_00011_dk5x2.1>",
>     "nextUri":"<http://localhost:8001/v1/statement/20140108_110629_00011_dk5x2/1>",
>     "columns": [ { "name":"name", "type":"varchar" } ],
>     "stats": { "state":"RUNNING", "scheduled":false,
>     "nodes":1, "totalSplits":0, "queuedSplits":0,
>     "runningSplits":0, "completedSplits":0, "cpuTimeMillis":0,
>     "wallTimeMillis":0, "processedRows":0, "processedBytes":0,
>     "rootStage": { "stageId":"0", "state":"SCHEDULED",
>     "done":false, "nodes":1, "totalSplits":0,
>     "queuedSplits":0, "runningSplits":0, "completedSplits":0,
>     "cpuTimeMillis":0, "wallTimeMillis":0, "processedRows":0,
>     "processedBytes":0, "subStages": [ { "stageId":"1",
>     "state":"SCHEDULED", "done":false, "nodes":1,
>     "totalSplits":0, "queuedSplits":0, "runningSplits":0,
>     "completedSplits":0, "cpuTimeMillis":0, "wallTimeMillis":0,
>     "processedRows":0, "processedBytes":0, "subStages":[] } ]
>     } }
>
> }
> ```
   


- GET /v1/statement/{queryId}/{token}


query queryId

:   初始POST返回给/v1/statement的查询标识

query token

:   初始POST返回给/v1/statement的令牌或先前调用返回给此调用的令牌。
当openLooKeng客户端提交语句执行时，openLooKeng创建一个查询，然后它向客户端返回一个nextUri。此调用与nextUri调用相对应，可以包含正在进行中的查询的状态更新，也可以将最终结果传递给客户端。

**请求样例**：

> ``` http
> GET /v1/statement/20140108_110629_00011_dk5x2/1 HTTP/1.1
> Host: localhost:8001
> User-Agent: StatementClient/0.55-SNAPSHOT
> ```

**响应样例**：

> ``` http
> HTTP/1.1 200 OK
>
> Content-Type: application/json X-Content-Type-Options: nosniff Vary:
> Accept-Encoding, User-Agent Transfer-Encoding: chunked
>
> 383 { "id":"20140108_110629_00011_dk5x2",
> "infoUri":"<http://localhost:8001/v1/query/20140108_110629_00011_dk5x2>",
> "columns": [ { "name":"name", "type":"varchar" } ],
> "data": [ ["<4165@domU-12-31-39-0F-CC-72>"] ], "stats": {
> "state":"FINISHED", "scheduled":true, "nodes":1,
> "totalSplits":2, "queuedSplits":0, "runningSplits":0,
> "completedSplits":2, "cpuTimeMillis":1, "wallTimeMillis":4,
> "processedRows":1, "processedBytes":27, "rootStage": {
> "stageId":"0", "state":"FINISHED", "done":true, "nodes":1,
> "totalSplits":1, "queuedSplits":0, "runningSplits":0,
> "completedSplits":1, "cpuTimeMillis":0, "wallTimeMillis":0,
> "processedRows":1, "processedBytes":32, "subStages": [ {
> "stageId":"1", "state":"FINISHED", "done":true, "nodes":1,
> "totalSplits":1, "queuedSplits":0, "runningSplits":0,
> "completedSplits":1, "cpuTimeMillis":0, "wallTimeMillis":4,
> "processedRows":1, "processedBytes":27, "subStages":[] } ] }
> } }
> ```


- DELETE /v1/statement/{queryId}/{token}

query queryId



:   初始POST返回给/v1/statement的查询标识

reqheader X-Presto-User

:   代表(optional)执行语句的用户

reqheader X-Presto-Source

:   查询来源

reqheader X-Presto-Catalog


:   执行查询基于的目录

reqheader X-Presto-Schema

:   执行查询的模式
