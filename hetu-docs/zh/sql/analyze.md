
# ANALYZE

## 摘要

``` sql
ANALYZE table_name [ WITH ( property_name = expression [, ...] ) ]
```

## 说明

收集给定表的表和列统计信息。

可选的 `WITH` 子句可用于提供特定于连接器的属性。要列出所有可用的属性，请运行以下查询：

    SELECT * FROM system.metadata.analyze_properties

目前仅 [Hive连接器](../connector/hive.md)支持该语句。

## 示例

分析表 `web` 以收集表和列统计信息：

    ANALYZE web;

分析目录 `hive` 和模式 `default` 中的表 `stores`：

    ANALYZE hive.default.stores;

分析 Hive 分区表 `sales` 中的分区 `'1992-01-01', '1992-01-02'`：

    ANALYZE hive.default.sales WITH (partitions = ARRAY[ARRAY['1992-01-01'], ARRAY['1992-01-02']]);

分析 Hive 分区表 `customers` 中具有复杂分区键（`state` 和 `city` 列）的分区。

    ANALYZE hive.default.customers WITH (partitions = ARRAY[ARRAY['CA', 'San Francisco'], ARRAY['NY', 'NY']]);