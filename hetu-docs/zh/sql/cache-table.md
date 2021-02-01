
# CACHE TABLE

## 摘要

``` sql
CACHE TABLE table_name WHERE condition;
```

## 说明

`CACHE TABLE` 使用连接器应缓存的表名和分区更新协调器元数据。

目前必须提供“ condition”，并且仅在分区列上定义。

## 示例

缓存“ CA”中商店的所有销售数据:

``` sql
    CACHE TABLE store_sales WHERE location = 'CA';
```
缓存2020年2月20日之后出售的商店中的所有销售数据。
 
``` sql 
    CACHE TABLE store_sales WHERE ss_sold_date_sk > 20200220;
```
缓存拥有复杂 condition 的数据
```sql
    CACHE TABLE store_sales WHERE location = 'CA' AND ss_sold_date_sk > 20200220;
```
 

## 限制

- 目前只有Hive连接器(ORC格式)支持此功能。 有关更多详细信息，请参见连接器文档。
- 目前 `WHERE` 中不支持 `LIKE` 字段。
- 目前不支持 `WHERE` 中的 `OR` 运算符。

## 另请参见

[SHOW CACHE](./show-cache.md)、[DROP CACHE](./drop-cache.md)