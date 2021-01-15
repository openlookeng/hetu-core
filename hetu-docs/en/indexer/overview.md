
# openLooKeng Heuristic Index

## Introduction

Indexes can be created using one or more columns of a database table, providing faster random lookups. Most Big Data formats such as ORC, Parquet and CarbonData already have indices embedded in them.

The Heuristic Indexer allows creating indexes on existing data but stores the index external to the original data source. This provides several benefits:

  - The index is agnostic to the underlying data source and can be used by any query engine
  - Existing data can be indexed without having to rewrite the existing data files
  - New index types not supported by the underlying data source can be created
  - Index data does not use the storage space of the data source


## Use cases

**Currently, Heuristic Index is only supports the Hive connector with 
tables using ORC storage format.**

### 1. Filtering scheduled Splits during query execution

*Index types supported: Bloom Index, Btree Index, MinMax Index*

When the engine needs to read data from a data source it schedules Splits. 
However, not all Splits will return data if a predicate is applied.

For example: `select * from test_base where j1='070299439'`

By keeping an external index for the predicate column, the Heuristic Indexer can determine whether each split contains the values being searched for and only schedule the read operation for the splits which possibly contain the value.

![indexer_filter_splits](../images/indexer_filter_splits.png)

### 2. Filtering Stripes when reading ORC files

*Index types supported: Bloom Index, MinMax Index*

Similar to Split filtering above, when using the Hive connector to read ORC tables,
Stripes can be filtered out based on the specified predicate. This reduces the amount
of data read and improves query performance.

### 3. Filtering rows when reading ORC files

*Index types supported: Bitmap Index*

Going one level lower, once the rows are read, they must be filtered if a predicate is present. 
This involves reading rows and then using the Filter operator to discard
rows that do not match the predicate.

By creating a Bitmap Index for the predicate column, the Heuristic Indexer will only read 
rows which match the predicate, before the Filter operator is even applied. This can reduce
memory and cpu usage and result in improved query performance, especially at higher concurrency.


## Getting started

This section gives a short tutorial which introduces the basic usage of Heuristic Index through a sample query.
For a complete list of configuration properties see [Properties](../admin/properties.md).

### 1. Configure indexer settings

In `etc/config.properties`, add these lines:

    hetu.heuristicindex.filter.enabled=true
    hetu.heuristicindex.filter.cache.max-memory=10GB
    hetu.heuristicindex.indexstore.uri=/opt/hetu/indices
    hetu.heuristicindex.indexstore.filesystem.profile=index-store-profile

Path whitelist：`["/tmp", "/opt/hetu", "/opt/openlookeng", "/etc/hetu", "/etc/openlookeng", current workspace]`

**Note**：
- `LOCAL` filesystem type is NOT supported anymore.
- `HDFS` filesystem type should be used in production in order for the index to be accessible by all nodes in the cluster.
- All nodes should be configured to use the same filesystem profile.
- Heuristic Index can be disabled while the engine is running by setting: `set session heuristicindex_filter_enabled=false;`

Filesystem profile file should be placed at `etc/filesystem/index-store-profile.properties`, 
where `index-store-profile` is the name referenced above in `config.properties`:

    fs.client.type=hdfs
    hdfs.config.resources=/path/to/core-site.xml,/path/to/hdfs-site.xml
    hdfs.authentication.type=NONE
    fs.hdfs.impl.disable.cache=true
    
If the kerberos authentication is enabled, additional configs `hdfs.krb5.conf.path`, 
`hdfs.krb5.keytab.path`, and `hdfs.krb5.principal` must be provided. 

With Heuristic Indexer configured, start the engine.

### 2. Identify a column to create index on

For a query like:

    SELECT * FROM hive.schema.table1 WHERE id="abcd1234";

Where `id` is unique, a Bloom Index on can significantly decrease the splits to read 
when scanning table1. 

### 3. Create index

To create index run the following statement:

    CREATE INDEX index_name USING bloom ON table1 (column);
    
**Note:**   Index for decimal type columns is not supported currently. For more details follow the [issue](https://gitee.com/openlookeng/hetu-core/issues/I2AMH0?from=project-issue)
### 4. Run query

After index is created, run the query; index will start loading in the background. 
Subsequent queries will utilize the index to reduce the amount of data read
 and query performance will be improved.


## Index Statements

See [Heuristic Index Statements](./hindex-statements.md).

-----

## Supported Index Types

| Index ID | Filtering type  | Best Column type                           | Supported query operators             | Notes                     | Example                                                                                                                                                                                                           |
|----------|-----------------|--------------------------------------------|---------------------------------------|---------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Bloom](./bloom.md)    | Split<br>Stripe | High cardinality<br>(such as an ID column) | `=` `IN`                                  |                           | `create index idx using bloom on hive.hindex.users (id);`<br>`select name from hive.hindex.users where id=123`                                                                                                    |
| [Btree](./btree.md)    | Split           | High cardinality<br>(such as an ID column) | `=` `>` `>=` `<` `<=` `IN` `BETWEEN` | Table must be partitioned | `create index idx using btree on hive.hindex.users (id) where regionkey IN (1,4) with ("level"='partition')`<br>(assuming table is partitioned on regionkey)<br>`select name from hive.hindex.users where id>123` |
| [MinMax](./minmax.md)   | Split<br>Stripe | Column which table is sorted on            | `=` `>` `>=` `<` `<=` |                           | `create index idx using bloom on hive.hindex.users (age);`<br>(assuming users is sorted by age)<br>`select name from hive.hindex.users where age>25`                                                              |
| [Bitmap](./bitmap.md)   | Row             | Low cardinality<br>(such as Gender column) | `=` `IN`                                  |                           | `create index idx using bitmap on hive.hindex.users (gender);`<br>`select name from hive.hindex.users where gender='female'`                                                                                      |

**Note:** unsupported operators will still function correctly but will not benefit from the index.


## Choosing Index Type

The Heuristic Indexer helps with queries where data is being filtered by a predicate.
Identify the column on which data is being filtered and use the decision flowchart to
help decide what type of index will work best.

Cardinality means the number of distinct values in the column relative to
number of total rows. For example, an `ID` column has a high cardinality
because IDs are unique. Whereas `employeeType` will have low cardinality
because there are likely only a few different types (e.g. Manager, Developer,
Tester).

![index-decision](../images/index-decision.png)

Example queries:

1. `SELECT id FROM employees WHERE site = 'lab';`

    In this query `site` has a low cardinality (i.e. not many sites) so **Bitmap Index** will help.

2. `SELECT * FROM visited WHERE id = '34857' AND date < '2020-01-01';`

    In this query `id` has a high cardinality (i.e. IDs are likely unique)
    and table is partitioned on `date` so **Btree Index** will help.
    
3. `SELECT * FROM salaries WHERE salary > 50251.40;`

    In this query `salary` has a high cardinality (i.e. salary of employees
    will slightly vary) and assuming `salaries` table is sorted on `salary`, 
    **MinMax Index** will help.

4. `SELECT * FROM assets WHERE id = 50;`

    In this query `id` has a high cardinality (i.e. IDs are likely unique)
    but the table is not partitioned, so **Bloom Index** will help.

5. `SELECT * FROM phoneRecords WHERE phone='1234567890' and type = 'outgoing' and date > '2020-01-01';`

    In this query `phone` has a high cardinality (i.e. there are many phone numbers, even if they
    made multiple calls), `type` has low cardinality (only outgoing or incoming),
    and the data is partitioned on date. Creating a **Btree Index** on `phone`
    and a **Bitmap Index** on `type` will help.

## Adding your own Index Type

See [Adding your own Index Type](./new-index.md).

## Access Control

See [Built-in System Access Control](../security/built-in-system-access-control.md).
