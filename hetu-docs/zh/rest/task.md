
任务资源
=============

任务资源提供了一组REST端点，这些端点使openLooKeng服务器能够就任务和任务输出进行交流。这不是一个供最终用户使用的服务，但它支持在openLooKeng上执行查询任务。

- GET /v1/task

返回openLooKeng服务器已知的所有任务的信息。

注意，对`/v1/task`的调用的输出可能相当大。如果你对忙碌的openLooKeng服务器执行这个命令，收到的响应将包括该服务器已知的每个任务的列表以及详细的操作符和驱动的统计信息。
为了便于本手册描述，下列的示例响应经过适当的删减。一个高负荷的openLooKeng服务器实际的响应输出可能有很多页。下面是一个状态为`CANCELED`的任务的`taskId`。

**响应样例**：

``` json
[ {
  "taskId" : "20131222_183944_00011_dk5x2.1.0",
  "version" : 9223372036854775807,
  "state" : "CANCELED",
  "self" : "unknown",
  "lastHeartbeat" : "2013-12-22T13:54:46.566-05:00",
  "outputBuffers" : {
    "state" : "FINISHED",
    "masterSequenceId" : 0,
    "pagesAdded" : 0,
    "buffers" : [ ]
  },
  "noMoreSplits" : [ ],
  "stats" : {
    "createTime" : "2013-12-22T13:54:46.566-05:00",
    "elapsedTime" : "0.00ns",
    "queuedTime" : "92.00us",
    "totalDrivers" : 0,
    "queuedDrivers" : 0,
    "runningDrivers" : 0,
    "completedDrivers" : 0,
    "memoryReservation" : "0B",
    "totalScheduledTime" : "0.00ns",
    "totalCpuTime" : "0.00ns",
    "totalBlockedTime" : "0.00ns",
    "rawInputDataSize" : "0B",
    "rawInputPositions" : 0,
    "processedInputDataSize" : "0B",
    "processedInputPositions" : 0,
    "outputDataSize" : "0B",
    "outputPositions" : 0,
    "pipelines" : [ ]
  },
  "failures" : [ ],
  "outputs" : { }
}]
```


- POST /v1/task/{taskId}


- DELETE /v1/task/{taskId}

从openLooKeng服务器删除指定的任务。


- GET /v1/task/{taskId}

通过`taskId`检索指定任务的信息。

下面是一个任务的输出结果示例。它包含以下为高级别部分：

-   `outputBuffers`
-   `noMoreSplits`
-   `stats`
-   `failures`
-   `outputs`

查询资源的响应中也有相同的输出。该响应列出了特定查询中涉及的所有阶段和任务。openLooKeng使用此调用协调查询。

**响应样例**：

``` json
{
"taskId" : "20140115_170528_00004_dk5x2.0.0",
"version" : 42,
"state" : "FINISHED",
"self" : "http://192.168.1.1:8080/v1/task/20140115_170528_00004_dk5x2.0.0",
"lastHeartbeat" : "2014-01-15T12:12:12.518-05:00",
"outputBuffers" : {
"state" : "FINISHED",
"masterSequenceId" : 0,
"pagesAdded" : 1,
"buffers" : [ {
  "bufferId" : "out",
  "finished" : true,
  "bufferedPages" : 0,
  "pagesSent" : 1
} ]
},
"noMoreSplits" : [ "8" ],
"stats" : {
"createTime" : "2014-01-15T12:12:08.520-05:00",
"startTime" : "2014-01-15T12:12:08.526-05:00",
"endTime" : "2014-01-15T12:12:12.518-05:00",
"elapsedTime" : "4.00s",
"queuedTime" : "6.39ms",
"totalDrivers" : 1,
"queuedDrivers" : 0,
"runningDrivers" : 0,
"completedDrivers" : 1,
"memoryReservation" : "174.76kB",
"totalScheduledTime" : "4.19ms",
"totalCpuTime" : "4.09ms",
"totalBlockedTime" : "29.50ms",
"rawInputDataSize" : "10.90kB",
"rawInputPositions" : 154,
"processedInputDataSize" : "10.90kB",
"processedInputPositions" : 154,
"outputDataSize" : "10.90kB",
"outputPositions" : 154,
"pipelines" : [ {
  "inputPipeline" : true,
  "outputPipeline" : true,
  "totalDrivers" : 1,
  "queuedDrivers" : 0,
  "runningDrivers" : 0,
  "completedDrivers" : 1,
  "memoryReservation" : "0B",
  "queuedTime" : {
    "maxError" : 0.0,
    "count" : 1.0,
    "total" : 5857000.0,
    "p01" : 5857000,
    "p05" : 5857000,
    "p10" : 5857000,
    "p25" : 5857000,
    "p50" : 5857000,
    "p75" : 5857000,
    "p90" : 5857000,
    "p95" : 5857000,
    "p99" : 5857000,
    "min" : 5857000,
    "max" : 5857000
  },
  "elapsedTime" : {
    "maxError" : 0.0,
    "count" : 1.0,
    "total" : 4.1812E7,
    "p01" : 41812000,
    "p05" : 41812000,
    "p10" : 41812000,
    "p25" : 41812000,
    "p50" : 41812000,
    "p75" : 41812000,
    "p90" : 41812000,
    "p95" : 41812000,
    "p99" : 41812000,
    "min" : 41812000,
    "max" : 41812000
  },
  "totalScheduledTime" : "4.19ms",
  "totalCpuTime" : "4.09ms",
  "totalBlockedTime" : "29.50ms",
  "rawInputDataSize" : "10.90kB",
  "rawInputPositions" : 154,
  "processedInputDataSize" : "10.90kB",
  "processedInputPositions" : 154,
  "outputDataSize" : "10.90kB",
  "outputPositions" : 154,
  "operatorSummaries" : [ {
    "operatorId" : 0,
    "operatorType" : "ExchangeOperator",
    "addInputCalls" : 0,
    "addInputWall" : "0.00ns",
    "addInputCpu" : "0.00ns",
    "addInputUser" : "0.00ns",
    "inputDataSize" : "10.90kB",
    "inputPositions" : 154,
    "getOutputCalls" : 1,
    "getOutputWall" : "146.00us",
    "getOutputCpu" : "137.90us",
    "getOutputUser" : "0.00ns",
    "outputDataSize" : "10.90kB",
    "outputPositions" : 154,
    "blockedWall" : "29.50ms",
    "finishCalls" : 0,
    "finishWall" : "0.00ns",
    "finishCpu" : "0.00ns",
    "finishUser" : "0.00ns",
    "memoryReservation" : "0B",
    "info" : {
  "bufferedBytes" : 0,
  "averageBytesPerRequest" : 11158,
  "bufferedPages" : 0,
  "pageBufferClientStatuses" : [ {
    "uri" : "http://192.168.1.1:8080/v1/task/20140115_170528_00004_dk5x2.1.0/results/ab68e201-3878-4b21-b6b9-f6658ddc408b",
    "state" : "closed",
    "lastUpdate" : "2014-01-15T12:12:08.562-05:00",
    "pagesReceived" : 1,
    "requestsScheduled" : 3,
    "requestsCompleted" : 3,
    "httpRequestState" : "queued"
  } ]
    }
  }, {
    "operatorId" : 1,
    "operatorType" : "FilterAndProjectOperator",
    "addInputCalls" : 1,
    "addInputWall" : "919.00us",
    "addInputCpu" : "919.38us",
    "addInputUser" : "0.00ns",
    "inputDataSize" : "10.90kB",
    "inputPositions" : 154,
    "getOutputCalls" : 2,
    "getOutputWall" : "128.00us",
    "getOutputCpu" : "128.64us",
    "getOutputUser" : "0.00ns",
    "outputDataSize" : "10.45kB",
    "outputPositions" : 154,
    "blockedWall" : "0.00ns",
    "finishCalls" : 5,
    "finishWall" : "258.00us",
    "finishCpu" : "253.19us",
    "finishUser" : "0.00ns",
    "memoryReservation" : "0B"
  }, {
    "operatorId" : 2,
    "operatorType" : "OrderByOperator",
    "addInputCalls" : 1,
    "addInputWall" : "438.00us",
    "addInputCpu" : "439.18us",
    "addInputUser" : "0.00ns",
    "inputDataSize" : "10.45kB",
    "inputPositions" : 154,
    "getOutputCalls" : 4,
    "getOutputWall" : "869.00us",
    "getOutputCpu" : "831.85us",
    "getOutputUser" : "0.00ns",
    "outputDataSize" : "10.45kB",
    "outputPositions" : 154,
    "blockedWall" : "0.00ns",
    "finishCalls" : 4,
    "finishWall" : "808.00us",
    "finishCpu" : "810.18us",
    "finishUser" : "0.00ns",
    "memoryReservation" : "174.76kB"
  }, {
    "operatorId" : 3,
    "operatorType" : "FilterAndProjectOperator",
    "addInputCalls" : 1,
    "addInputWall" : "166.00us",
    "addInputCpu" : "166.66us",
    "addInputUser" : "0.00ns",
    "inputDataSize" : "10.45kB",
    "inputPositions" : 154,
    "getOutputCalls" : 5,
    "getOutputWall" : "305.00us",
    "getOutputCpu" : "241.14us",
    "getOutputUser" : "0.00ns",
    "outputDataSize" : "10.90kB",
    "outputPositions" : 154,
    "blockedWall" : "0.00ns",
    "finishCalls" : 2,
    "finishWall" : "70.00us",
    "finishCpu" : "71.02us",
    "finishUser" : "0.00ns",
    "memoryReservation" : "0B"
  }, {
    "operatorId" : 4,
    "operatorType" : "TaskOutputOperator",
    "addInputCalls" : 1,
    "addInputWall" : "50.00us",
    "addInputCpu" : "51.03us",
    "addInputUser" : "0.00ns",
    "inputDataSize" : "10.90kB",
    "inputPositions" : 154,
    "getOutputCalls" : 0,
    "getOutputWall" : "0.00ns",
    "getOutputCpu" : "0.00ns",
    "getOutputUser" : "0.00ns",
    "outputDataSize" : "10.90kB",
    "outputPositions" : 154,
    "blockedWall" : "0.00ns",
    "finishCalls" : 1,
    "finishWall" : "35.00us",
    "finishCpu" : "35.39us",
    "finishUser" : "0.00ns",
    "memoryReservation" : "0B"
  } ],
  "drivers" : [ ]
} ]
},
"failures" : [ ],
"outputs" : { }
}
```



- GET /v1/task/{taskId}/results/{outputId}/{token}

openLooKeng通过此服务检索任务输出。


- DELETE /v1/task/{taskId}/results/{outputId}
openLooKeng通过此服务删除任务输出。


