REFRESH META CACHE
==================

使用方式
--------

```sql
REFRESH META CACHE [FOR CATALOG]
```

说明
-----------

`REFRESH META CACHE`用于刷新当前目录连接器元数据缓存。`REFRESH META CACHE FOR CATALOG`用于刷新指定目录连接器元数据缓存。

示例
--------

刷新当前目录连接器元数据缓存

```sql
REFRESH META CACHE
```

刷新指定目录连接器元存储缓存

```sql
REFRESH META CACHE FOR catalog
```

限制
-----------

目前仅Hive连接器支持该功能。