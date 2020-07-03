+++

title = "CACHE TABLE"
+++

# CACHE TABLE

## 摘要

``` sql
CACHE TABLE table_name WHERE condition
```

## 说明

`CACHE TABLE` 使用连接器应缓存的表名和分区更新协调器元数据。

必须提供 `condition`，并且仅在分区列上对其进行定义。

## 示例

缓存表：

    CACHE TABLE store_sales where location = 'CA';

## 限制

仅 Hive 连接器支持该功能。有关更多详细信息，请参见连接器文档。

## 另请参见

[show-cache](./show-cache.html)
[drop-cache](./drop-cache.html)