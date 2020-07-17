
# Teradata函数

这些函数提供与Teradata SQL的兼容性

## 字符串函数

**char2hexint(string)** -> varchar

返回字符串的UTF-16BE编码的十六进制表示形式。

**index(string, substring)** -> bigint

`strpos`函数的别名。

**substring(string, start)** -> varchar

`substr`函数的别名。

**substring(string, start, length)** -> varchar

`substr`函数的别名。

## 日期函数

该部分中的函数使用与Teradata日期时间函数兼容的格式字符串。下表基于Teradata参考手册，描述了支持的格式说明符：

| 说明符| 说明| 
|:----------|:----------| 
| `- / , . ; :`| 忽略标点符号| 
| `dd`| 一个月中的第几日(1–31)| 
| `hh`| 一天中的第几个小时(1–12)| 
| `hh24`| 一天中的第几个小时(0–23)| 
| `mi`| 分钟(0–59)| 
| `mm`| 月份（01–12）| 
| `ss`| 秒(0–59)| 
| `yyyy`| 四位年份| 
| `yy`| 两位年份| 

**警告**

当前不支持不区分大小写。所有说明符必须为小写。

**to\_char(timestamp**, format) -> varchar

使用`format`将`timestamp`格式化为一个字符串。

**to\_timestamp(string**, format) -> timestamp

使用`format`将`string`解析为`TIMESTAMP`。

**to\_date(string**, format) -> date

使用`format`将`string`解析为`DATE`。