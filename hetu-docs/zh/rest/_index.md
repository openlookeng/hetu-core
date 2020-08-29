# openLooKeng REST接口说明

本章介绍openLooKeng REST API的定义。openLooKeng使用REST进行openLooKeng安装中的所有通信。基于JSON的REST服务可以促进客户机与openLooKeng协调器之间的通信，也可以用于openLooKeng协调器与多个openLooKeng工作节点之间的通信。在本章中，您将找到openLooKeng提供的API的详细描述以及请求和响应的示例。

## REST接口简介

在openLooKeng中，所有内容都以REST API的形式公开在openLooKeng中，HTTP是所有组件相互通信的方法。

openLooKeng REST API包含若干高级资源，这些资源对应于openLooKeng安装的组件。

查询资源

> 查询资源采用SQL查询。它位于`/v1/query`目录下，接受多种HTTP方法。

节点资源

> 节点资源返回openLooKeng安装的worker节点信息。路径：/v1/node。

阶段资源

> 当openLooKeng协调器接收到一个查询时，它会创建多个阶段的分布式系统，这些阶段相互协作以执行一个查询。阶段资源用于协调器创建相应阶段的网络。它还被分阶段使用，以协调彼此。

语句资源

> 客户端执行语句的标准资源。当执行一条语句时，openLooKeng客户端会重复调用这个资源来获取正在执行的语句的状态以及已完成语句的结果。

任务资源

> Stage包含若干组件，其中一个组件是任务。内部组件使用这个资源来协调Stage的执行。
