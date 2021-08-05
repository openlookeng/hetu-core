Memory Connector
================

The Memory Connector stores data and metadata in RAM on workers to allow for fast queries. 
Data and metadata are spilled to local disk and automatically reloaded if nodes are restarted.

Configuration
-------------

### Memory Connector Configuration

To configure the Memory Connector, create or modify the catalog properties file `etc/catalog/memory.properties` for the Memory Connector.
For example, you can write:

``` properties
connector.name=memory
memory.max-data-per-node=200GB
memory.spill-path=/opt/hetu/data/spill          
```   

**Note:**
- `spill-path` should be set to a directory with enough free space to hold
 the table data.
- See **Configuration Properties** section for additional properties and
 details.
- In `etc/config.properties` ensure that `task.writer-count` is set to
 `>=` number of nodes in the cluster running openLooKeng. This will help
  distribute the data uniformly between all the workers.
- Hetu Metastore must be configured. The default settings are included in
  `etc/hetu-metastore.properties`. 
    - Check [Hetu Metastore](../admin/meta-store.md) for more information. 
- State Store must be configured to enable automatic memory refresh on workers.
    - Check [State Store](../admin/state-store.md) for more information
    - Automatic memory refresh will allow Memory Connector to clean unused tables more often resulting in more efficient use of memory.

Examples
--------

Create a table using the Memory Connector:

    CREATE TABLE memory.default.nation AS
    SELECT * from tpch.tiny.nation;

Insert data into a table in the Memory Connector:

    INSERT INTO memory.default.nation
    SELECT * FROM tpch.tiny.nation;

Select from the Memory Connector:

    SELECT * FROM memory.default.nation;

Drop table:

    DROP TABLE memory.default.nation;

Create a table using the Memory Connector with sorting, indices and spill compression:

    CREATE TABLE memory.default.nation
    WITH (
        sorted_by=array['nationkey'],
        index_columns=array['name', 'regionkey'],
        spill_compression=true
    )
    AS SELECT * from tpch.tiny.nation;

After table creation completes, the Memory Connector will start building indices and sorting data in the background. Once the processing is complete any queries using the sort or index columns will be faster and more efficient.

For now, `sorted_by` only accepts a single column.


Configuration Properties
------------------------

| Property Name                         | Default Value   | Required| Description               |
|---------------------------------------|-----------------|---------|---------------------------|
| `memory.spill-path                   `  | None          | Yes     | Directory where memory data will be spilled to. Must have enough free space to store the tables. SSD preferred.|
| `memory.max-data-per-node            `  | 256MB         | Yes     | Memory limit for total data stored on this node  |
| `memory.max-logical-part-size        `  | 256MB         | No      | Memory limit for each LogicalPart. Default value is recommended.|
| `memory.max-page-size                `  | 512KB         | No      | Memory limit for each page. Default value is recommended.|
| `memory.logical-part-processing-delay`  | 5s            | No      | The delay between when the table is created/updated and LogicalPart processing starts. Default value is recommended.|
| `memory.thread-pool-size             `  | Half of threads available to the JVM | No      | Maximum threads to allocate for background processing (e.g. sorting, index creation, cleanup, etc)|

Path whitelist：`["/tmp", "/opt/hetu", "/opt/openlookeng", "/etc/hetu", "/etc/openlookeng", current workspace]`

Additonal WITH properties
--------------
Use these properties when creating a table with the Memory Connector to make queries faster.

| Property Name            | Argument type             | Requirements                     | Description|
|--------------------------|---------------------------|----------------------------------|------------|
| sorted_by                | `array['col']`            | Maximum of one column. Column type must be comparable.  | Sort and create indexes on the given column|
| index_columns            | `array['col1', 'col2']`   | None                             | Create indexes on the given column|
| spill_compression        | `boolean`           | None                             | Compress data when spilling to disk|


Index Types
--------------
These are the types of indices that are built on the columns you specify in `sorted_by` or `index_columns`. If a query operator is not supported by a particular index you can still
use that operator, but the query will not benefit from the index.

| Index ID                          |Built for Columns In | Supported query operators             |
|-----------------------------------|----------------------------------------|---------------------------------------|
| Bloom   | `sorted_by,index_columns`                                 | `=` `IN`                             |                   
| MinMax  | `sorted_by,index_columns`                            | `=` `>` `>=` `<` `<=` `IN` `BETWEEN` |
| Sparse  | `sorted_by`                            | `=` `>` `>=` `<` `<=` `IN` `BETWEEN` |

Developer Information
----------------------------

This section outlines the overall design of the Memory Connector, as shown in the figure below.

![Memory Connector Overall Design](../images/memory-connector-design.png)

### Scheduling Process
The data to be processed are stored in pages, which are distributed to different worker nodes in openLooKeng.
In the Memory Connector, each worker has a number of LogicalParts.
During table creation, LogicalParts in the workers are filled with the input pages in a round-robin fashion.
Table data will be automatically spilled to disk as part of a background process as well. 
If there is not enough memory to hold the entire data, the tables can be released from memory according to LRU rule.
HetuMetastore is used to persist table metadata.
At query time, when Tablescan operation is scheduled, the LogicalParts will be scheduled.

### LogicalPart
As shown in the lower part of the design figure, LogicalPart is the data structure that contains both indexes and data.
The sorting and indexing are handled in a background process allowing faster querying,
but the table is still queriable during processing.
LogicalParts have a maximum configurable size (default 256 MB). 
New LogicalParts are created once the previous one is full.


### Indices
Bloom filter, sparse index and MinMax index are created in the LogicalPart.
Based on the pushed down predicate, entire LogicalParts can be filtered out using the Bloom Filter and MinMax indices.
Further Page filtering is done using the Sparse index.
Pages are first sorted, then optimized and finally a Sparse Index is created. 
This allows for smaller index sizes since not all unique values need to be stored. The Sparse index
helps reduce input rows but does not perform perfect filtering. 
Further filtering is done by openLooKeng’s Filter Operator.
Referring to the Sparse Index example above, this is how the Memory Connector would filter data for different queries:

```
For query: column=a.
Return Page 0 and 1 (note: contains extra b row).

For query: column=b.
Return Page 2 and Page 1.

For query: column=c.
Return floor entry of c (Page 2).

For query: column=d.
No pages need to be returned because last value of floor entry of c (Page 2) is less-than d.

For queries containing > >= < <= BETWEEN IN similar logic is applied.
```

Limitations and known Issues
---------------------------------------------

- After `DROP TABLE`, memory is not released immediately. It is released on next `CREATE TABLE` operation.
    - A simple workaround is to create a small temporary table to trigger a cleanup `CREATE TABLE memory.default.tmp AS SELECT * FROM tpch.tiny.nation;`
- Currently only a single column in ascending order is supported by `sorted_by`
- If a CTAS (CREATE TABLE AS) query fails or is cancelled, an invalid table will remain. This table must be dropped manually.