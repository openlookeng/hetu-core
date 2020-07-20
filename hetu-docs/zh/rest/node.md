
节点资源
=============



- GET /v1/node

返回openLooKeng服务器已知的节点列表。此调用不需要标头的查询参数，它只返回一个数组，数组中包含openLooKeng安装中的每个已知节点。

响应消息中，`recentRequests`、`recentFailures`和`recentSuccesses`的取值是随时间指数衰减的计数器，衰减参数为1分钟。这个衰减率意味着，如果一个openLooKeng节点在几秒内有1000次成功，那么这个统计值在一分钟内就会下降到367次。

`age`显示一个节点的运行时长，uri指向该节点的HTTP服务器。最后一次请求和响应时间显示了一个节点的最近使用情况。

下面的响应示例显示一个未经历了任何故障条件的节点。各节点同时上报流量正常、故障等统计数据。

**响应样例**：

> ``` http
> HTTP/1.1 200 OK
> Vary: Accept
> Content-Type: text/javascript
>
> [
> {
>     "uri":"http://192.168.1.1:8080",
> "recentRequests":25.181940555111073,
> "recentFailures":0.0,
> "recentSuccesses":25.195472984170983,
> "lastRequestTime":"2013-12-22T13:32:44.673-05:00",
> "lastResponseTime":"2013-12-22T13:32:44.677-05:00",
> "age":"14155.28ms",
> "recentFailureRatio":0.0,
> "recentFailuresByType":{}
> }
>
> ]
> ```


如果一个节点发生故障，您会看到如下所示的响应。此例中，一个节点发生了一连串的错误。recentFailuresByType字段列出了节点上发生的Java异常。

**错误响应样例**：

> ``` http
> HTTP/1.1 200 OK
>
> Vary: Accept Content-Type: text/javascript
>
> [
>
> {
>
>    ​    "age": "4.45m", "lastFailureInfo": { "message":
>​     "Connect Timeout", "stack": [
>    ​     "org.eclipse.jetty.io.ManagedSelector$ConnectTimeout.run(ManagedSelector.java:683)",
>    ​     .... "java.lang.Thread.run(Thread.java:745)" ],
>    ​     "suppressed": [], "type":
>    ​     "java.net.SocketTimeoutException" }, "lastRequestTime":
>    ​     "2017-08-05T11:53:00.647Z", "lastResponseTime":
>    ​     "2017-08-05T11:53:00.647Z", "recentFailureRatio":
>    ​     0.47263053472046446, "recentFailures": 2.8445543205610617,
>    ​     "recentFailuresByType": {
>    ​     "java.net.SocketTimeoutException": 2.8445543205610617 },
>    ​     "recentRequests": 6.018558073577414, "recentSuccesses":
>    ​     3.1746446343010297, "uri": "<http://172.19.0.3:8080>"
>    
> }
>
> ]
> ```



- GET /v1/node/failed

调用此服务将返回一个JSON文档，其中列出了所有上次心跳检测失败的节点。由此调用返回的信息与前一个业务返回的信息相同。

**响应样例**：

> ``` json
> 
> [
> {
>
> "age": "1.37m", "lastFailureInfo": { "message":
> "Connect Timeout", "stack": [
> "org.eclipse.jetty.io.ManagedSelector$ConnectTimeout.run(ManagedSelector.java:683)",
> ...
> "java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)",
> "java.lang.Thread.run(Thread.java:745)" ], "suppressed":
> [], "type": "java.net.SocketTimeoutException" },
> "lastRequestTime": "2017-08-05T11:52:42.647Z",
> "lastResponseTime": "2017-08-05T11:52:42.647Z",
> "recentFailureRatio": 0.22498784153043677,
> "recentFailures": 20.11558290058638,
> "recentFailuresByType": {
> "java.net.SocketTimeoutException": 20.11558290058638 },
> "recentRequests": 89.40742203558189, "recentSuccesses":
> 69.30583024727453, "uri": "<http://172.19.0.3:8080>"
>    
> }
> ]
> ```
