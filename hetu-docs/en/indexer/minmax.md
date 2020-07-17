
# Minmax Index

## Use cases

MinMax Index is used for split filtering, and is used only by the **coordinator** nodes.

If this index exists on a column which is part of a predicate in the query, the engine may be able to improve performance by filtering scheduled splits similar to Bloom Index.

For example if an index exists on column

`age`

and the query is

```sql
select * from table where age > 50
```



*Tip: sorting the data on the index column will provide the best results*