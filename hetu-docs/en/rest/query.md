
Query Resource
==============

The Query REST service is the most complex of the rest services. It
contains detailed information about nodes, and other details that
capture the state and history of a query being executed on a openLooKeng
installation.

- GET /v1/query

This service returns information and statistics about queries that are currently
executed on a openLooKeng coordinator.

When you point a web browser at a openLooKeng coordinate, you can find a rendered
version of the output from this service which displays recent queries that have executed on a openLooKeng installation.


- GET /v1/query/{queryId}

If you look to gather very detailed statistics about a query, this is the service you would call. If you load the web interface of a openLooKeng coordinator you can see a list of current queries. Clicking on a
query reveals a link to this service.

**Example response**:

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

