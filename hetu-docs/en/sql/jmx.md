JMX
===

Synopsis
--------

```sql
SELECT * FROM jmx.current."table_name";
```

Description
-----------

All JMX metrics are stored as tables in schema `current` of catalog `jmx`. `table_name` must be put in quotation mark.

Users need to find the table corresponding to the jmx metrics first, and then run `SELECT` queries to view these metrics.

Examples
--------

View row data cache stats:
```sql
    SELECT * FROM jmx.current."io.prestosql.orc:name=hive,type=rowdatacachestatslister";
```
View bloom filter cache stats:
```sql
    SELECT * FROM jmx.current."io.prestosql.orc:name=hive,type=bloomfiltercachestatslister";
```