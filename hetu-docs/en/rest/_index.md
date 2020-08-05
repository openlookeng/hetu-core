# openLooKeng REST API


This chapter defines the openLooKeng REST API. openLooKeng uses REST for all
communication within a openLooKeng installation. JSON-based REST services
facilitate communication between the client and the openLooKeng coordinator as
well as for communicate between a openLooKeng coordinator and multiple openLooKeng
workers. In this chapter you will find detailed descriptions of the APIs
offered by openLooKeng as well as example requests and responses.

## REST API Overview

In openLooKeng, everything is exposed as a REST API in openLooKeng and HTTP is the
method by which all component communicate with each other.

The openLooKeng REST API contains several, high-level resources that correspond
to the components of a openLooKeng installation.

Query Resource

> The query resource takes a SQL query. It is available at the path
> `/v1/query` and accepts several HTTP methods.

Node Resource

> The node resource returns information about worker nodes in a openLooKeng
> installation. It is available at the path `/v1/node`.

Stage Resource

> When a openLooKeng coordinator receives a query it creates distribute system
> of stages which collaborate with one another to execute a query. The
> Stage resource is used by the coordinator to create a network of
> corresponding stages. It is also used by stages to coordinate with one
> another.

Statement Resource

> This is the standard resource used by the client to execute a
> statement. When executing a statement, the openLooKeng client will call this
> resource repeatedly to get the status of an ongoing statement
> execution as well as the results of a completed statement.

Task Resource

> A stage contains a number of components, one of which is a task. This
> resource is used by internal components to coordinate the execution of
> stages.
