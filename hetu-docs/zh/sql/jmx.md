JMX
===

摘要
--------

```sql
SELECT * FROM jmx.current."table_name";
```

说明
-----------

所有的JMX数据都以表格的形式存储在`jmx.current`中，其中`table_name`必须用引号注释。

用户需要先找到存储 JMX 数据的表格，然后使用`SELECT`语句进行查看。

示例
--------

查看 row data cache 统计数据：
```sql
    SELECT * FROM jmx.current."io.prestosql.orc:name=hive,type=rowdatacachestatslister";
```
查看 bloom filter cache 统计数据：
```sql
    SELECT * FROM jmx.current."io.prestosql.orc:name=hive,type=bloomfiltercachestatslister";
```