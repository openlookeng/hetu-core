
# Resource Groups


Resource groups place limits on resource usage, and can enforce queueing policies on queries that run within them or divide their resources among sub-groups. A query belongs to a single resource group, and consumes resources from that group (and its ancestors). Except for the limit on queued queries, when a resource group runs out of a resource it does not cause running queries to fail; instead new queries become queued. A resource group may have sub-groups or may accept queries, but may not do both.

The resource groups and associated selection rules are configured by a manager which is pluggable. Add an `etc/resource-groups.properties` file with the following contents to enable the built-in manager that reads a JSON config file:

``` properties
resource-groups.configuration-manager=file
resource-groups.config-file=etc/resource-groups.json
```

Change the value of `resource-groups.config-file` to point to a JSON config file, which can be an absolute path, or a path relative to the openLooKeng data directory.

### Additional Configuration

In addition to above properties in `etc/resource-groups.properties`, below two properties can be configured, which gets used along with kill policy (details of same as part of killPolicy)

`resource-groups.memory-margin-percent` (optional)- This is the allowed percentage of memory variation between two queries considered to be same. In this case queries does not get ordered based on memory usage instead they get ordered based on query execution progress provided query progress difference is more than configured. Default value is 10%.

`resource-groups.query-progress-margin-percent`(optional)- This is the allowed percentage of query execution progress between two queries considered to be same. In this query does not get ordered based on query execution progress. Default value is 5%.

## Resource Group Properties


-   `name` (required): name of the group. May be a template (see below).
-   `maxQueued` (required): maximum number of queued queries. Once this limit is reached new queries will be rejected.
-   `hardConcurrencyLimit` (required): maximum number of running queries.
-   `softMemoryLimit` (required): maximum amount of distributed memory this group may use before new queries become queued. May be specified as an absolute value (i.e. `1GB`) or as a percentage (i.e. `10%`) of the cluster\'s memory.
-   `softCpuLimit` (optional): maximum amount of CPU time this group may use in a period (see `cpuQuotaPeriod`) before a penalty will be applied to the maximum number of running queries. `hardCpuLimit` must also be specified.
-   `hardCpuLimit` (optional): maximum amount of CPU time this group may use in a period.
-   `schedulingPolicy` (optional): specifies how queued queries are selected to run, and how sub-groups become eligible to start their queries. May be one of three values. When High Availability (muliple coordinators) mode is enabled, only `fair` scheduling policy will be supported:
    - `fair` (default): queued queries are processed first-in-first-out, and sub-groups must take turns starting new queries (if they have any queued).
    - `weighted_fair`: sub-groups are selected based on their `schedulingWeight` and the number of queries they are already running concurrently. The expected share of running queries for a sub-group is computed based on the weights for all currently eligible sub-groups. The sub-group with the least concurrency relative to its share is selected to start the next query.
    - `weighted`: queued queries are selected stochastically in proportion to their priority (specified via the `query_priority` [session property](../sql/set-session.md)). Sub groups are selected to start new queries in proportion to their `schedulingWeight`.
    - `query_priority`: all sub-groups must also be configured with `query_priority`. Queued queries will be selected strictly according to their priority.
-   `schedulingWeight` (optional): weight of this sub-group. See above. Defaults to `1`.
-   `jmxExport` (optional): If true, group statistics are exported to JMX for monitoring. Defaults to `false`.
-   `subGroups` (optional): list of sub-groups.
-   `killPolicy`(optional): Specifies how running queries are selected to kill If overall memory usage exceed **softMemoryLimit** after queries being submitted to worker. 

    - `no_kill` (default): Queries will not be killed.
    - `recent_queries`: This means queries in the reverse order of execution will be killed. 
    - `oldest_queries`: Queries in the order of execution will get killed.
    - `high_memory_queries` : Queries in the order of memory usage will be killed. Query having higher memory usage will get killed first so that with minimum number of query kill, more memory gets freed.
    As part of this policy, we try to balance memory usage and percentage completion. So if two queries memory usage are within 10% (configurable as resource-groups.memory-margin-percent) of limit, then we pick the query which has progressed (% of execution) less. Incase these two queries difference in terms of percentage of completion are within 5% (configurable as resource-groups.query-progress-margin-percent) then we chose based on memory itself.
    - `finish_percentage_queries`:  Query in the order of percentage of query execution will be killed. Query with least percentage of execution will be killed first.

## Selector Rules

-   `user` (optional): regex to match against user name.
-   `source` (optional): regex to match against source string.
-   `queryType` (optional): string to match against the type of the query submitted:
- `DATA_DEFINITION`: Queries that alter/create/drop the metadata of schemas/tables/views, and that manage prepared statements, privileges, sessions, and transactions.
    - `DELETE`: `DELETE` queries.
- `DESCRIBE`: `DESCRIBE`, `DESCRIBE INPUT`, `DESCRIBE OUTPUT`, and `SHOW` queries.
    - `EXPLAIN`: `EXPLAIN` queries.
    - `INSERT`: `INSERT` and `CREATE TABLE AS` queries.
    - `SELECT`: `SELECT` queries.
-   `clientTags` (optional): list of tags. To match, every tag in this
    list must be in the list of client-provided tags associated with the
    query.
-   `group` (required): the group these queries will run in.

Selectors are processed sequentially and the first one that matches will be used.

## Global Properties


-   `cpuQuotaPeriod` (optional): the period in which cpu quotas are enforced.

## Providing Selector Properties

The source name can be set as follows:

- CLI: use the `--source` option.
- JDBC: set the `ApplicationName` client info property on the `Connection` instance.

Client tags can be set as follows:

- CLI: use the `--client-tags` option.
- JDBC: set the `ClientTags` client info property on the `Connection` instance.

## Throttling and Kill Queries

It may happen that once query got submitted to worker, then memory limit exceeded, in that case we need to handle these running queries using below mechanism:

- Throttle Queries
- Kill Queries

### Throttle Queries

Throttling of new split schedule is done to stop further increase in memory at worker. If memory usage by current query resource group has already exceeded **softReservedMemory** then further new splits will not get scheduled till memory usage comes below **softReservedMemory**.

**softReservedMemory** is recommended to configure lesser than **softMemoryLimit**.

User can also chose to **disable throttling** by omitting **softReservedMemory** configuration for a group.

### Kill Queries

If query could not get throttle and memory usage exceed softMemoryLimit, then query will be killed (made to fail) as per the kill policy configured. Only queries running from leaf group are considered to kill.

## Example


-   In the example configuration below, there are several resource groups, some of which are templates. Templates allow administrators to construct resource group trees dynamically. For example, in the `pipeline_${USER}` group, `${USER}` will be expanded to the name of the user that submitted the query. `${SOURCE}` is also supported, which will be expanded to the source that submitted the query. You may also use custom named variables in the `source` and `user` regular expressions.
    
     
    
    There are four selectors that define which queries run in which resource group:
    
     
    
    > - The first selector matches queries from `bob` and places them in the admin group.
    > - The second selector matches all data definition (DDL) queries from a source name that includes `pipeline` and places them in the `global.data_definition` group. This could help reduce queue times for this class of queries, since they are expected to be fast.
    > - The third selector matches queries from a source name that includes `pipeline`, and places them in a dynamically-created per-user pipeline group under the `global.pipeline` group.
    > - The fourth selector matches queries that come from BI tools which have a source matching the regular expression `jdbc#(?.*)`, and have client provided tags that are a superset of `hi-pri`. These are placed in a dynamically-created sub-group under the `global.pipeline.tools` group. The dynamic sub-group will be created based on the named variable `toolname`, which is extracted from the in the regular expression for source. Consider a query with a source `jdbc#powerfulbi`, user `kayla`, and client tags `hipri` and `fast`. This query would be routed to the `global.pipeline.bi-powerfulbi.kayla` resource group.
    > - The last selector is a catch-all, which places all queries that have not yet been matched into a per-user adhoc group.
    
     
    
    
    
    Together, these selectors implement the following policy:
    
     
    
    - The user `bob` is an admin and can run up to 50 concurrent queries. Queries will be run based on user-provided priority.
    
     
    
    For the remaining users:
    
     
    
    - No more than 100 total queries may run concurrently.
    - Up to 5 concurrent DDL queries with a source `pipeline` can run. Queries are run in FIFO order.
    - Non-DDL queries will run under the `global.pipeline` group, with a total concurrency of 45, and a per-user concurrency of 5. Queries are run in FIFO order.
    - For BI tools, each tool can run up to 10 concurrent queries, and each user can run up to 3. If the total demand exceeds the limit of 10, the user with the fewest running queries will get the next concurrency slot. This policy results in fairness when under contention.
    - All remaining queries are placed into a per-user group under `global.adhoc.other` that behaves similarly.


[resource-groups-example.json](resource-groups-example.json)

