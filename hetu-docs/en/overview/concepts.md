
# openLooKeng Concepts

## Server Types

There are two types of openLooKeng servers: coordinators and workers. The following section explains the difference between the two.

### Coordinator

The openLooKeng coordinator is the server that is responsible for parsing statements, planning queries, and managing openLooKeng worker nodes. It is the \"brain\" of a openLooKeng installation and is also the node to which a client
connects to submit statements for execution. Every openLooKeng installation must have a openLooKeng coordinator alongside one or more openLooKeng workers. For development or testing purposes, a single instance of openLooKeng can be configured to perform both roles.

The coordinator keeps track of the activity on each worker and coordinates the execution of a query. The coordinator creates a logical model of a query involving a series of stages which is then translated into a series of connected tasks running on a cluster of openLooKeng workers.

Coordinators communicate with workers and clients using a REST API.

### Worker

A openLooKeng worker is a server in a openLooKeng installation which is responsible for executing tasks and processing data. Worker nodes fetch data from connectors and exchange intermediate data with each other. The
coordinator is responsible for fetching results from the workers and returning the final results to the client.

When a openLooKeng worker process starts up, it advertises itself to the discovery server in the coordinator, which makes it available to the openLooKeng coordinator for task execution.

Workers communicate with other workers and openLooKeng coordinators using a REST API.

## Data Sources

Throughout this documentation, you\'ll read terms such as connector, catalog, schema, and table. These fundamental concepts cover openLooKeng\'s model of a particular data source and are described in the following section.

### Connector

A connector adapts openLooKeng to a data source such as Hive or a relational database. You can think of a connector the same way you think of a driver for a database. It is an implementation of openLooKeng\'s [SPI](../develop/spi-overview.md) which allows openLooKeng to interact with a resource using a standard API.

openLooKeng contains several built-in connectors: a connector for [JMX](../connector/jmx.md) , a [System](../connector/system.md) connector which provides access to built-in system tables, a [Hive](../connector/hive.md) connector, and a [TPCH](../connector/tpch.md) connector designed to serve TPC-H benchmark data. Many third-party developers have contributed connectors so that openLooKeng can access data in a variety of data sources.

Every catalog is associated with a specific connector. If you examine a catalog configuration file, you will see that each contains a mandatory property `connector.name` which is used by the catalog manager to create a connector for a given catalog. It is possible to have more than one catalog use the same connector to access two different instances of a similar database. For example, if you have two Hive clusters, you can configure two catalogs in a single openLooKeng cluster that both use the Hive connector, allowing you to query data from both Hive clusters (even within the same SQL query).

### Catalog

A openLooKeng catalog contains schemas and references a data source via a connector. For example, you can configure a JMX catalog to provide access to JMX information via the JMX connector. When you run a SQL
statement in openLooKeng, you are running it against one or more catalogs.
Other examples of catalogs include the Hive catalog to connect to a Hive data source.

When addressing a table in openLooKeng, the fully-qualified table name is always rooted in a catalog. For example, a fully-qualified table name of `hive.test_data.test` would refer to the `test` table in the `test_data` schema in the `hive` catalog.

Catalogs are defined in properties files stored in the openLooKeng configuration directory.

### Schema

Schemas are a way to organize tables. Together, a catalog and schema define a set of tables that can be queried. When accessing Hive or a relational database such as MySQL with openLooKeng, a schema translates to the same concept in the target database. Other types of connectors may choose to organize tables into schemas in a way that makes sense for the underlying data source.

### Table

A table is a set of unordered rows which are organized into named columns with types. This is the same as in any relational database. The mapping from source data to tables is defined by the connector.



## Query Execution Model

openLooKeng executes SQL statements and turns these statements into queries that are executed across a distributed cluster of coordinator and workers.

### Statement

openLooKeng executes ANSI-compatible SQL statements. When the openLooKeng documentation refers to a statement, it is referring to statements as defined in the ANSI SQL standard which consists of clauses, expressions, and predicates.

Some readers might be curious why this section lists separate concepts for statements and queries. This is necessary because, in openLooKeng, statements simply refer to the textual representation of a SQL statement. When a statement is executed, openLooKeng creates a query along with a query plan that is then distributed across a series of openLooKeng workers.

### Query

When openLooKeng parses a statement, it converts it into a query and creates a distributed query plan which is then realized as a series of interconnected stages running on openLooKeng workers. When you retrieve information about a query in openLooKeng, you receive a snapshot of every component that is involved in producing a result set in response to a statement.

The difference between a statement and a query is simple. A statement can be thought of as the SQL text that is passed to openLooKeng, while a query refers to the configuration and components instantiated to execute that statement. A query encompasses stages, tasks, splits, connectors, and other components and data sources working in concert to produce a result.

### Stage

When openLooKeng executes a query, it does so by breaking up the execution into a hierarchy of stages. For example, if openLooKeng needs to aggregate data from one billion rows stored in Hive, it does so by creating a root stage to aggregate the output of several other stages all of which are designed to implement different sections of a distributed query plan.

The hierarchy of stages that comprises a query resembles a tree. Every query has a root stage which is responsible for aggregating the output from other stages. Stages are what the coordinator uses to model a
distributed query plan, but stages themselves don\'t run on openLooKeng workers.

### Task

As mentioned in the previous section, stages model a particular section of a distributed query plan, but stages themselves don\'t execute on openLooKeng workers. To understand how a stage is executed, you\'ll need to
understand that a stage is implemented as a series of tasks distributed over a network of openLooKeng workers.

Tasks are the \"work horse\" in the openLooKeng architecture as a distributed query plan is deconstructed into a series of stages which are then translated to tasks which then act upon or process splits. A openLooKeng task
has inputs and outputs, and just as a stage can be executed in parallel by a series of tasks, a task is executing in parallel with a series of drivers.

### Split

Tasks operate on splits which are sections of a larger data set. Stages at the lowest level of a distributed query plan retrieve data via splits from connectors, and intermediate stages at a higher level of a distributed query plan retrieve data from other stages.

When openLooKeng is scheduling a query, the coordinator will query a connector for a list of all splits that are available for a table. The coordinator keeps track of which machines are running which tasks and what splits are being processed by which tasks.

### Driver

Tasks contain one or more parallel drivers. Drivers act upon data and combine operators to produce output that is then aggregated by a task and then delivered to another task in another stage. A driver is a sequence of operator instances, or you can think of a driver as a physical set of operators in memory. It is the lowest level of parallelism in the openLooKeng architecture. A driver has one input and one output.

### Operator

An operator consumes, transforms and produces data. For example, a table scan fetches data from a connector and produces data that can be consumed by other operators, and a filter operator consumes data and produces a subset by applying a predicate over the input data.

### Exchange

Exchanges transfer data between openLooKeng nodes for different stages of a query. Tasks produce data into an output buffer and consume data from other tasks using an exchange client.
