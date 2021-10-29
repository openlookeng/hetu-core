
Statement Resource
==================

- POST /v1/statement

query 

: SQL Query to execute

reqheader X-Presto-User

: User to execute statement on behalf of (optional)

reqheader X-Presto-Source

: Source of query

reqheader X-Presto-Catalog

: Catalog to execute query against

reqheader X-Presto-Schema

: Schema to execute query against

Submits a statement to openLooKeng for execution. The openLooKeng client executes
queries on behalf of a user against a catalog and a schema. When you run
a query with the openLooKeng CLI, it is calling out to the statement resource
on the openLooKeng coordinator.

The request to the statement resource is the SQL query to execute as a
post along with the standard X-Presto-Catalog, X-Presto-Source,
X-Presto-Schema, and X-Presto-User headers.

The response from the statement resource contains a query identifier
which can be used to gather detailed information about a query. This
initial response also includes information about the stages that have
been created to execute this query on openLooKeng workers. Every query has a
root stage and the root stage is given a stage identifier of \"0\" as
shown in the following example response.

This root stage aggregates the responses from other stages running on
openLooKeng workers and delivers them to the client through the openLooKeng coordinator.
When a client receives a response to this POST it will contain a "nextUri\" property which directs the client to query this address for additional results from the query.

**Example request**:

> ``` http
> POST /v1/statement HTTP/1.1
>
> Host: localhost:8001 X-Presto-Catalog: jmx X-Presto-Source: presto-cli
> X-Presto-Schema: jmx User-Agent: StatementClient/0.55-SNAPSHOT
> X-Presto-User: tobrie1 Content-Length: 41
>
> select name from "java.lang:type=runtime"
> ```


**Example response**:

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

:   The query identifier returned from the initial POST to /v1/statement

query token

:   The token returned from the initial POST to /v1/statement or from a
    previous call to this same call

When a openLooKeng client submits a statement for execution, openLooKeng creates a
query and then it returns a nextUri to the client. This call corresponds
to that nextUri call and can contain either a status update for a query
in progress or it can deliver the final results to the client.

**Example request**:

> ``` http
> GET /v1/statement/20140108_110629_00011_dk5x2/1 HTTP/1.1
> Host: localhost:8001
> User-Agent: StatementClient/0.55-SNAPSHOT
> ```

**Example response**:

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

: The query identifier returned from the initial POST to /v1/statement

reqheader X-Presto-User

: User to execute statement on behalf of (optional)

reqheader X-Presto-Source

:  Source of query

reqheader X-Presto-Catalog

: Catalog to execute query against

reqheader X-Presto-Schema

: Schema to execute query against
