
# Bitmap Index

## Use cases

Bitmap Index is used for filtering data read from ORC files and is used only by the **worker** nodes

- If this index exists on a column which is part of a predicate in the query, the performance may be improved while reading the ORC files.

For example, if an index exists on column `country` and the query is

``` sql
select * from table where country="China"
```

- This index works best if the column's values are not too distinct (e.g. country) and are distributed.

For example, assume that the table stores information about where users are from and the table data is in 10 files. There maybe be several users from a particular country, so each file will have some users from the country. If we create a bitmap index on the country column, we can perform filtering early on while reading the data files. i.e. the predicate is pushed down to the reading of the file. Without this index, all the data files will need to be read into memory as Pages and then the filtering would happen. With the index, we can ensure that the Pages already only contain the rows matching the predicate. This can help reduce the memory and CPU usage and can result in improved performance when many concurrent queries are running.

