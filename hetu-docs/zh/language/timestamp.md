
旧时间戳和新时间戳
========================

新的`TIMESTAMP`和`TIME`语义使类型与SQL标准保持一致。详见以下章节。


**注意**

*新的`TIMESTAMP`语义仍在试验中。建议保持原有`TIMESTAMP`语义为启用状态。您可以通过全局配置或基于每个会话配置新的语义来验证新语义。在新版本中，可能会废弃旧版的语义。*


配置
-------------

可以使用`deprecated.legacy-timestamp`配置属性启用旧语义。将其设置为`true`（默认）将启用旧语义，而将其设置为`false`将启用新语义。

此外，可以通过`legacy_timestamp`会话属性实现基于会话的语义启用或禁用。

### TIMESTAMP语义变化

以前，`TIMESTAMP`类型描述的是openLooKeng会话时区中的时间实例。现在，openLooKeng将`TIMESTAMP`值视为一组表示实际时间的字段：

-   `YEAR OF ERA`
-   `MONTH OF YEAR`
-   `DAY OF MONTH`
-   `HOUR OF DAY`
-   `MINUTE OF HOUR`
-   `SECOND OF MINUTE` - as `DECIMAL(5, 3)`

因此，除非明确需要某个时区，例如在转换为`TIMESTAMP WITH TIME ZONE` 或 `TIME WITH TIME ZONE`时，`TIMESTAMP`值不会以任何方式与会话时区链接。在这些情况下，如SQL标准中所约定的，将会使用会话时区的时区偏移量。


### TIME语义变化

`TIME`类型变得与`TIMESTAMP`类型相似。

### TIME with TIME ZONE语义变化

由于兼容性要求，`TIME WITH TIME ZONE`还不可能与SQL标准完全对齐。因此，在计算`TIME WITH TIME ZONE`的时区偏移量时，openLooKeng使用会话的开始日期和时间。


在使用`TIME WITH TIME ZONE`的查询中可以看到，查询的时区已经发生了时区策略更改或使用了夏令时。例如，会话开始时间为2017-03-01：

-   查询: `SELECT TIME '10:00:00 Asia/Kathmandu' AT TIME ZONE 'UTC'`
-   旧的查询结果: `04:30:00.000 UTC`
-   新的查询结果: `04:15:00.000 UTC`
