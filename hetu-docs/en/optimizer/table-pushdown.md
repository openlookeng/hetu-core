# Table Pushdown Optimizer Rule

openLooKeng supports Table Pushdown optimizer rule to improve latency for eligible queries.

The user must execute the following command to generate stats for all the tables which are part of the SQL query so that the join sources are reordered based on the size of the tables-

```sql
ANALYZE tableName;
```

**User Hint Comment-**

The user may also add a special comment in the following format to leverage the benefit from the Table Pushdown rule by specifying the table names and their distinct(primary key) column names.

```sql
/* #distinct@ table1 = col1, col2, ... #*/
```

This hint is optional as openLooKeng can identify all distinct columns if the relevant statistics are available for the table.

The following query (modified from Query 17 of TPC-H benchmark) is one such eligible query and pushes the outer table into the subquery and improves overall query latency.

**Original Query-**

```sql
SELECT 
  Sum(lineitem.extendedprice) / 7.0 AS avg_yearly 
FROM 
  lineitem, 
  part, 
(
  SELECT 
    0.2 * Avg(lineitem.quantity) AS s_avg, lineitem.partkey AS s_partkey 
  FROM 
    lineitem 
  GROUP BY 
    lineitem.partkey 
) 
WHERE 
  part.partkey = lineitem.partkey 
AND 
  part.brand = 'Brand#43' 
AND 
  part.container = 'LG PACK' 
AND 
  part.partkey = s_partkey 
AND 
  lineitem.quantity < s_avg /* #distinct@ part = partkey #*/;
```

In the above query, table `part` is the relevant outer query table with `partkey` as the unique column, being joined with table `lineitem`. An equivalent rewritten query, with `part`  pushed into the subquery is given as follows.

**Equivalent Rewritten Query-**

```sql
SELECT 
  Sum(lineitem.extendedprice) / 7.0 AS avg_yearly 
FROM 
  lineitem, 
(
  SELECT 
    0.2 * Avg(lineitem.quantity) AS s_avg, lineitem.partkey AS s_partkey 
  FROM 
    lineitem, part 
  WHERE 
    part.brand = 'Brand#43' 
  AND 
    part.container = 'LG PACK' 
  AND 
    part.partkey = lineitem.partkey 
  GROUP BY 
    lineitem.partkey
) 
WHERE 
  s_partkey = lineitem.partkey 
AND 
  lineitem.quantity < s_avg;
```

The feature is disabled by default. The user can enable it by executing the following command to set the session parameter-

```sql
SET SESSION push_table_through_subquery = true;
```

