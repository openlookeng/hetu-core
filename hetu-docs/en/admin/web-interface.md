
# Web Interface


openLooKeng provides a web interface for monitoring and managing queries. The web interface is accessible on the openLooKeng coordinator via HTTP, using the HTTP port number specified in the coordinator  `config_properties`.

The main page has a list of queries along with information like unique query ID, query text, query state, percentage completed, username and source from which this query originated. The currently running queries are at the top of the page, followed by the most recently completed or failed queries.

The possible query states are as follows:

-   `QUEUED` \-- Query has been accepted and is awaiting execution.
-   `PLANNING` \-- Query is being planned.
-   `STARTING` \-- Query execution is being started.
-   `RUNNING` \-- Query has at least one running task.
-   `BLOCKED` \-- Query is blocked and is waiting for resources (buffer space, memory, splits, etc.).
-   `FINISHING` \-- Query is finishing (e.g. commit for autocommit queries).
-   `FINISHED` \-- Query has finished executing and all output has been consumed.
-   `FAILED` \-- Query execution failed.

The `BLOCKED` state is normal, but if it is persistent, it should be investigated. It has many potential causes: insufficient memory or splits, disk or network I/O bottlenecks, data skew (all the data goes to a few workers), a lack of parallelism (only a few workers available), or computationally expensive stages of the query following a given stage.
Additionally, a query can be in the `BLOCKED` state if a client is not processing the data fast enough (common with \"SELECT \*\" queries).

For more detailed information about a query, simply click the query ID link. The query detail page has a summary section, graphical representation of various stages of the query and a list of tasks. Each task ID can be clicked to get more information about that task.

The summary section has a button to kill the currently running query. There are two visualizations available in the summary section: task execution and timeline. The full JSON document containing information
and statistics about the query is available by clicking the *JSON* link. These visualizations and other statistics can be used to analyze where time is being spent for a query.

## General Properties

### `hetu.queryeditor-ui.allow-insecure-over-http`

> -   **Type:** `boolean`
> -   **Allowed values:** `true`, `false`
> -   **Default value:** `false`
>
> Insecure authentication over HTTP is disabled by default. This could be overridden via `hetu.queryeditor-ui.allow-insecure-over-http` property of `etc/config.properties` (e.g. hetu.queryeditor-ui.allow-insecure-over-http=true).

### `hetu.queryeditor-ui.execution-timeout`

> -   **Type:** `duration`
> -   **Default value:** `100 DAYS`
>
> UI Execution timeout is set to 100 days as default. This could be overridden via `hetu.queryeditor-ui.execution-timeout` of `etc/config.properties`

### `hetu.queryeditor-ui.max-result-count`

> - **Type:** `int`
> - **Default value:** `1000`
>
> UI max result count is set to 1000 as default. This could be overridden via `hetu.queryeditor-ui.max-result-count` of `etc/config.properties`

### `hetu.queryeditor-ui.max-result-size-mb`

>- **Type:** `size`
>- **Default value:** `1GB`
>
> UI max result size is set to 1 GB as default. This could be overridden via `hetu.queryeditor-ui.max-result-size-mb` of `etc/config.properties`

### `hetu.queryeditor-ui.session-timeout`

> -   **Type:** `duration`
> -   **Default value:** `1 DAYS`
>
> UI session timeout is set to 1 day as default. This could be overridden via `hetu.queryeditor-ui.session-timeout` of `etc/config.properties`