# Star-Tree 

Star tree cubing is a pre-aggregation technique to achieve low latency runtime for iceberg queries. An iceberg query computes an aggregate function over 
an attribute ( or set of attributes) in order to find aggregate values above a specified threshold. Using this technique, User is provided with an option to 
create a cube with necessary aggregations and dimensions. Then, when aggregation queries are executed, the cube is used to executing the query instead of the 
original table. The actual performance gain is achieved during the TableScan operation as cubes are pre-computed and pre-aggregated. 

For this reason, the cubing technique is highly effective when the group by cardinality results in lesser rows than the original table.

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

### Building Cube for Large dataset
One of the limitations with the current implementation is that Cube cannot be built for a larger dataset at once. This is due to the cluster memory limitation.
Processing large number rows requires more memory than cluster is configured with. This results in query failing with message "Query exceeded per-node user memory 
limit". To overcome this issue, "Insert into cube" query support was added. The user has ability to build a cube for larger data by executing multiple 
insert into cube statements. The insert statement accepts a where clause, and it can be used to limit the number of processed and inserted into cube.

This section explains the steps to build a cube for larger dataset. 

Let us take an example of TPCDS dataset and `store_sales` table. The table has 10 years worth of data 
and user wants to build a cube for year 2001 and due to the cluster memory limit, the entire data set for year 2001 cannot be processed at once.

```sql

CREATE CUBE store_sales_cube ON store_sales WITH (AGGREGATIONS = (sum(ss_net_paid), sum(ss_sales_price), sum(ss_quantity)), GROUP = (ss_sold_date_sk, ss_store_sk));

SELECT min(d_date_sk) as year_start, max(d_date_sk) as year_end FROM date_dim WHERE d_year = 2001;
 year_start | year_end 
------------+----------
    2451911 |  2452275 
(1 row)

INSERT INTO CUBE store_sales_cube WHERE ss_sold_date_sk BETWEEN 2451911 AND 242275; 
-- This could result in query failure if the number of rows need to be processed is huge and the query memory exceeds the configured limit.

To overcome this issue, multiple insert statements can be used into process rows and insert into cube and the number of rows canbe controlled by using where clause;

INSERT INTO CUBE store_sales_cube WHERE ss_sold_date_sk BETWEEN 2451911 AND 2452010;
INSERT INTO CUBE store_sales_cube WHERE ss_sold_date_sk >= 2452011 AND ss_sold_date_sk <= 2452110;
INSERT INTO CUBE store_sales_cube WHERE ss_sold_date_sk BETWEEN 2452111 AND 2452210;
INSERT INTO CUBE store_sales_cube WHERE ss_sold_date_sk BETWEEN 2452211 AND 2452275;

Internally the system will rewrite and merge all continuous range predicates into a single predicate;

SHOW CUBES;

           Cube Name             |         Table Name         | Status |         Dimensions          |                     Aggregations                      |                                     Where Clause                                     
---------------------------------+----------------------------+--------+-----------------------------+-------------------------------------------------------+-------------------------------------------------------+------------------------------
 hive.tpcds_sf1.store_sales_cube | hive.tpcds_sf1.store_sales | Active | ss_sold_date_sk,ss_store_sk | sum(ss_sales_price),sum(ss_net_paid),sum(ss_quantity) | (("ss_sold_date_sk" >= BIGINT '2451911') AND ("ss_sold_date_sk" < BIGINT '2452276')) 

Note:
1. The system will try to rewrite all type of Predicates into a Range to see if they can be merged together. 
   All continous predicates will be merged into a single range predicate and remainining predicates are untouched.
   Only the following types are supported and can be merged together. 
   Integer, TinyInt, SmallInt, BigInt, Date;
   For other types, its difficult to identify if two predicates are continous therefore they cannot be merged together. And because of this issue, there is 
   possibility that particular cube may not be used during query optimisation even if the cube has all the required data. For example,
   
   INSERT INTO CUBE store_sales_cube WHERE store_id BETWEEN 'A01' AND 'A10';
   INSERT INTO CUBE store_sales_cube WHERE store_id BETWEEN 'A11' AND 'A20';
   
   Here these two predicates cannot be merged into store_id BETWEEN 'A01' AND 'A20'; So the cube won't be used 
   for queries that are spanning over two the predicates;
   
   SELECT ss_store_id, sum(ss_sales_price) WHERE ss_store_id BETWEEN 'A05' AND 'A15'; - Cube won't be used for optimizing this query. This is a limitation as of now.

2. Only Single column predicates can be merged. 
```

## Limitation

1. Star tree cube is only effective when the group by cardinality is considerably lower than the number of rows in 
   source table.
2. A significant amount of user effort required in maintaining Cubes for large datasets.
3. Only incremental insert into cube is supported. Cannot delete specific rows from Cube.