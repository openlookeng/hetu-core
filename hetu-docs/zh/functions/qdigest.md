
# 分位数摘要函数

openLooKeng使用分位数摘要数据结构实现`approx_percentile`函数。基础数据结构[qdigest](../language/types.md)在openLooKeng中公开为一种数据类型，可以独立于`approx_percentile`对其进行创建、查询和存储。

## 数据结构

分位点摘要是存储近似百分位信息的数据概要。该数据结构的openLooKeng类型称为`qdigest`，它接受一个参数，该参数必须是`bigint`、`double`或`real`，它们表示可以由`qdigest`获取的数字集合。可以在不损失精度的情况下对其进行合并，还可以将其与`VARBINARY`来回转换，以进行存储和检索。

## 函数

**merge(qdigest)** -> qdigest

将所有输入`qdigest`合并为单个`qdigest`。

**value\_at\_quantile(qdigest(T), quantile)** -> T

在给定介于0和1之间的数字`quantile`的情况下从分位数摘要返回近似百分位值。

**values\_at\_quantiles(qdigest(T), quantiles)** -> T

在给定输入分位数摘要和0和1之间的值数组（表示要返回的分位数）的情况下以数组的形式返回近似百分位值。

**qdigest\_agg(x)** -> qdigest\<\[与x相同]>

返回由`x`的所有输入值组成的`qdigest`。

**qdigest\_agg(x, w)** -> qdigest\<\[与x相同]>

返回由`x`的所有输入值（使用每项权重`w`）组成的`qdigest`。

**qdigest\_agg(x, w, accuracy)** -> qdigest\<\[与x相同]>

返回由`x`的所有输入值（使用每项权重`w`和最大误差`accuracy`）组成的`qdigest`。`accuracy`必须是一个大于0且小于1的值，并且对于所有输入行是一个常量。