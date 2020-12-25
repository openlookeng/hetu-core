
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

## Connector Properties File Configuration

The web interface supports expanding and adding new connectors in the interface by setting the connector properties file.The connector properties file can be set by referring to the template`presto-main/etc/connector_properties.json.template`.

`etc/connector_properties.json` is the default path of the web connector configuration file，you can change the path by set and modify `hetu.queryeditor-ui.server.connector-properties-json` property of `etc/config.properties`.

The web connector configuration file supports the following properties:

| Property Name | Description| Required |
|----------|----------|----------|
| `connectorWithProperties` | Yes | Connector with properties |
| `user` | Yes | The user who created the connector |
| `uuid` | Yes | Connector unique identification |
| `docLink` | Yes | Link to official introduction document of the connector |
| `configLink` | Yes | Link to official configuration document of the connector |

The properties that `connectorWithProperties` supported are as follows:

| Property Name | Description| Required | Default Value |
|----------|----------|----------|----------|
| `connectorName`| Yes | Connector name | |
| `connectorLabel`| Yes | Connector label | |
| `propertiesEnabled` | No | Whether to enable the connector properties configuration panel or not | false |
| `catalogConfigFilesEnabled` | No | Whether to enable properties file upload for current connector configuration or not | false |
| `globalConfigFilesEnabled` | No | Whether to enable properties file upload for system configuration (system configuration are shared by all connectors) or not | false |
| `properties` | Yes | Connector properties | |

The properties that `properties` supported are as follows:

| Property Name | Description| Required | Default Value |
|----------|----------|----------|----------|
| `name`| Yes | Property name | |
| `value`| Yes | Property value | |
| `description` | Yes | Property description | |
| `required` | No | Property required | false |
| `readOnly` | No | If the read-only mode is enabled, the value modification is not supported | false |
| `type` | Yes | Property type | |

Example, datacenter connector properties configuration of the connector properties file is as follow：

``` json
[
  {
    "connectorWithProperties": {
      "connectorName": "dc",
      "connectorLabel": "DataCenter: Query data in remote OpenLooKeng data center",
      "propertiesEnabled": true,
      "catalogConfigFilesEnabled": false,
      "globalConfigFilesEnabled": false,
      "properties": [
        {
          "name": "connection-url",
          "value": "http://localhost:8080",
          "description": "The connection URL of remote OpenLooKeng data center",
          "required" : true,
          "type" : "string"
        },
        {
          "name": "connection-user",
          "value": "lk",
          "description": "User to connect to remote data center",
          "required" : true,
          "readOnly": true,
          "type": "string"
        }
      ]
    }, 
    "user": "lk",
    "uuid": "ff35a9cf-8cfc-4ff5-be2f-a6389c144f9e",
    "docLink": "https://openlookeng.io/docs/docs/connector/datacenter.html",
    "configLink": "https://openlookeng.io/docs/docs/connector/datacenter.html#configuration"
  },
  ......
]
```