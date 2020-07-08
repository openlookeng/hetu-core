+++

title = "DROP CACHE"
+++

# DROP CACHE

## 摘要

``` sql
DROP CACHE table
```

## 说明

`DROP CACHE`仅从协调器删除`table`的缓存元数据。 到期时间自动清除工作人员的缓存 或达到大小限制，但重复进行的拆分将不会重用任何缓存的节点分配。

## 示例

删除“ sales”表的缓存元数据

```sql 
    DROP CACHE sales
```

## 限制

目前只有Hive连接器支持此功能。 有关更多详细信息，请参见连接器文档。

## 另请参见

[cache-table](./cache-table.html)、[show-cache](./show-cache.html)