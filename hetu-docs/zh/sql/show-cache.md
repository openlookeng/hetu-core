
# SHOW CACHE

## 摘要

``` sql
SHOW CACHE;
```

## 说明

`SHOW CACHE` 显示分离式缓存协调器元数据。分离式缓存包含有关缓存的表和分区的信息。

## 示例

显示所有缓存元数据

```sql
    SHOW CACHE;
```

显示销售表的缓存元数据

```sql
    SHOW CACHE sales;
```  

## 限制

目前只有Hive连接器支持此功能。 有关更多详细信息，请参见连接器文档。

## 另请参见

[CACHE TABLE](./cache-table.md)、[DROP CACHE](./drop-cache.md)