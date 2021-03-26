# Star-Tree 

Star tree cubing is a pre-aggregation technique to achieve low latency runtime for iceberg queries. Star tree cubing aimed to reduce 
latency for iceberg queries. An iceberg query computes an aggregate function over an attribute ( or set of attributes) in order to 
find aggregate values above a specified threshold.

## Supported functions
    COUNT, COUNT DISTINCT, MIN, MAX, SUM, AVG

## Enabling and Disabling Star-tree
To enable:
```sql 
SET SESSION enable_star_tree_index=true;
```
To disable:
```sql 
SET SESSION enable_star_tree_index=false;
```

## Configuration Properties
| Property Name                                     | Default Value       | Required| Description|
|---------------------------------------------------|---------------------|---------|--------------|
| optimizer.enable-star-tree-index                  | false               | No      | Enables star-tree index|
| cube.metadata-cache-size                          | 5                   | No      | The maximum number of metadata for star-trees that could be loaded into cache before eviction happens|
| cube.metadata-cache-ttl                           | 1h                  | No      | The maximum time to live of star-trees that are be loaded into cache before eviction happens |

## Examples

Creating a star-tree cube:
```sql 
CREATE CUBE nation_cube 
ON nation 
WITH (AGGREGATIONS=(count(*), count(distinct regionkey), avg(nationkey), max(regionkey)),
GROUP=(nationkey),
format='orc', partitioned_by=ARRAY['nationkey']);
```
Next, to add data to the cube:
```sql 
INSERT INTO CUBE nation_cube WHERE nationkey > 5;
```
To use the new cube, just query the original table using aggregations that were included in the cube:
```sql 
SELECT count(*) FROM nation WHERE nationkey > 5 GROUP BY nationkey;
SELECT nationkey, avg(nationkey), max(regionkey) WHERE nationkey > 5 GROUP BY nationkey;
```

## Optimizer Changes

The star tree aggregation rule is an Iterative optimizer that optimizes the logical plan by replacing the original aggregation sub-tree
and original table scan with pre-aggregation table scan. This optimizer uses the TupleDomain construct to match if predicates provided in the Query can
be supported by the Cubes. The exact rows are not queried to check if Cube is applicable or not.


## Dependencies

Star Tree index relies on Hetu metastore to store the cube related metadata.
Please check [Hetu Metastore](../admin/meta-store.md) for more information.

## Limitation

1. Star tree cube is only effective when the group by cardinality is considerably lower than the number of rows in 
   source table.
2. A significant amount of user effort required in maintaining Cubes for large datasets.
3. Only incremental insert into cube is supported. Cannot delete specific rows from Cube.