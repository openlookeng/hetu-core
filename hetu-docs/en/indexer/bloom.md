
# Bloom Index

## Use cases

Bloom Index is used for split filtering, and is used only by the **coordinator** nodes.

- If this index exists on a column which is part of a predicate in the query, openLooKeng may be able to improve performance by filtering scheduled splits.

For example, if an index exists on column `id` and the query is:

```sql
select * from table where id=12345
```



- Bloom index works best if the column's values are unique (e.g. userid) and are not too distributed.

For example, assume the tables stores information about users and the table data is in 10 files.  For a given userid, only one file will contain the data. Therefore creating an index on userid will help us to filter out 9 out of the 10 files at scheduling time and will save significant IO time that would've  therwise been used to read each of the files.

*Tip: if possible, it is recommended to sort the data on the column being indexed.*