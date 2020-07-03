+++

title = "DROP CACHE"
+++

# DROP CACHE

## 摘要

``` sql
DROP CACHE table
```

## 说明

`DROP CACHE` 从协调器中删除 `table` 的缓存元数据。

## 示例

删除缓存：

    DROP CACHE table

## 限制

仅 Hive 连接器支持该功能。有关更多详细信息，请参见连接器文档。

## 另请参见

[cache-table](./cache-table.html)、[show-cache](./show-cache.html)