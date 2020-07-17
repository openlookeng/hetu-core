
查询资源
==============

Query REST服务是rest服务中最复杂的。它包含节点的详细信息，以及其它在openLooKeng上执行的查询的状态和历史的详细信息。

- GET /v1/query

此服务返回有关当前在openLooKeng协调节点上执行的查询的信息和统计信息。

当你将浏览器指向一个openLooKeng坐标时，你会看到此服务输出的一个渲染的版本，将显示最近的在openLooKeng上执行的查询。
   

- GET /v1/query/{queryId}

可调用此服务收集详细的有关查询的统计数据。如果您加载openLooKeng协调节点的Web界面，您会看到一个关于当前查询的清单。单击查询将显示指向此服务的链接。

**响应样例**：

> ``` json
> {
> "queryId" : "20131229_211533_00017_dk5x2",
> "session" : {
>       "user" : "tobrien",
>       "source" : "openlk-cli",
>       "catalog" : "jmx",
>       "schema" : "jmx",
>       "remoteUserAddress" : ""192.168.1.1"",
>       "userAgent" : "StatementClient/0.55-SNAPSHOT",
>       "startTime" : 1388351852026
> },
> "state" : "FINISHED",
> "self" : "http://127.0.0.1:8080/v1/query/20131229_211533_00017_dk5x2",
> "fieldNames" : [ "name" ],
> "query" : "select name from \"java.lang:type=runtime\"",
> "queryStats" : {
>       "createTime" : "2013-12-29T16:17:32.027-05:00",
>       "executionStartTime" : "2013-12-29T16:17:32.086-05:00",
>       "lastHeartbeat" : "2013-12-29T16:17:44.561-05:00",
>       "endTime" : "2013-12-29T16:17:32.152-05:00",
>       "elapsedTime" : "125.00ms",
>       "queuedTime" : "1.31ms",
>       "analysisTime" : "4.84ms",
>       "distributedPlanningTime" : "353.00us",
>       "totalTasks" : 2,
>       "runningTasks" : 0,
>       "completedTasks" : 2,
>       "totalDrivers" : 2,
>       "queuedDrivers" : 0,
>       "runningDrivers" : 0,
>       "completedDrivers" : 2,
>       "totalMemoryReservation" : "0B",
>       "totalScheduledTime" : "5.84ms",
>       "totalCpuTime" : "710.49us",
>       "totalBlockedTime" : "27.38ms",
>       "rawInputDataSize" : "27B",
>       "rawInputPositions" : 1,
>       "processedInputDataSize" : "32B",
>       "processedInputPositions" : 1,
>       "outputDataSize" : "32B",
>       "outputPositions" : 1
> },
> "outputStage" : ...
> }
> ```
   
