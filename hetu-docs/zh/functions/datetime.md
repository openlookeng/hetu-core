
# 日期和时间函数和运算符

## 日期和时间运算符

| 运算符 | 示例                                                | 结果                      |
| :----- | :-------------------------------------------------- | :------------------------ |
| `+`    | `date '2012-08-08' + interval '2' day`              | `2012-08-10`              |
| `+`    | `time '01:00' + interval '3' hour`                  | `04:00:00.000`            |
| `+`    | `timestamp '2012-08-08 01:00' + interval '29' hour` | `2012-08-09 06:00:00.000` |
| `+`    | `timestamp '2012-10-31 01:00' + interval '1' month` | `2012-11-30 01:00:00.000` |
| `+`    | `interval '2' day + interval '3' hour`              | `2 03:00:00.000`          |
| `+`    | `interval '3' year + interval '5' month`            | `3-5`                     |
| `-`    | `date '2012-08-08' - interval '2' day`              | `2012-08-06`              |
| `-`    | `time '01:00' - interval '3' hour`                  | `22:00:00.000`            |
| `-`    | `timestamp '2012-08-08 01:00' - interval '29' hour` | `2012-08-06 20:00:00.000` |
| `-`    | `timestamp '2012-10-31 01:00' - interval '1' month` | `2012-09-30 01:00:00.000` |
| `-`    | `interval '2' day - interval '3' hour`              | `1 21:00:00.000`          |
| `-`    | `interval '3' year - interval '5' month`            | `2-7`                     |

## 时区转换

`AT TIME ZONE`运算符设置时间戳的时区：

    SELECT timestamp '2012-10-31 01:00 UTC';
    2012-10-31 01:00:00.000 UTC
    
    SELECT timestamp '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles';
    2012-10-30 18:00:00.000 America/Los_Angeles

## 日期和时间函数

**current\_date** -> date

返回查询开始时的当前日期。

**current\_time** -> time with time zone

返回查询开始时的当前时间。

**current\_timestamp** -> timestamp with time zone

返回查询开始时的当前时间戳。

**current\_timezone()** -> varchar

以IANA定义的格式（如`America/Los_Angeles`）或相对于UTC的固定偏移量（如`+08:35`）返回当前时区。

**date(x)** -> date

这是`CAST(x AS date)`的别名。

**from\_iso8601\_timestamp(string)** -> timestamp with time zone

将以ISO 8601格式表示的`string`解析为`timestamp with time zone`。

**from\_iso8601\_date(string)** -> date

将以ISO 8601格式表示的`string`解析为`date`。

**from\_unixtime(unixtime)** -> timestamp

将UNIX时间戳`unixtime`作为时间戳返回。`unixtime`是从`1970-01-01 00:00:00`开始经历的秒数。

**from\_unixtime(unixtime, string)** -> timestamp with time zone

将UNIX时间戳`unixtime`作为timestamp with time zone返回，其中使用`string`作为时区。`unixtime`是从`1970-01-01 00:00:00`开始经历的秒数。

**from\_unixtime(unixtime, hours, minutes)** -> timestamp with time zone

将UNIX时间戳`unixtime`作为timestamp with time zone返回，其中使用`hours`和`minutes`作为时区偏移量。`unixtime`是从`1970-01-01 00:00:00`开始经历的秒数。

**localtime** -> time

返回查询开始时的当前时间。

**localtimestamp** -> timestamp

返回查询开始时的当前时间戳。

**now()** -> timestamp with time zone

这是`current_timestamp`的别名。

**to\_iso8601(x)** -> varchar

将`x`格式化为ISO 8601字符串。`x`可以是date、timestamp或timestamp with time zone。

**to\_milliseconds(interval)** -> bigint

以毫秒为单位返回以天和秒为单位的间隔`interval`。

**to\_unixtime(timestamp)** -> double

将`timestamp`作为UNIX时间戳返回。

**注意**

以下SQL标准函数不使用括号：

- `current_date`
- `current_time`
- `current_timestamp`
- `localtime`
- `localtimestamp`

## 截断函数

`date_trunc`函数支持以下单位：

| 单位      | 截断值示例                |
| :-------- | :------------------------ |
| `second`  | `2001-08-22 03:04:05.000` |
| `minute`  | `2001-08-22 03:04:00.000` |
| `hour`    | `2001-08-22 03:00:00.000` |
| `day`     | `2001-08-22 00:00:00.000` |
| `week`    | `2001-08-20 00:00:00.000` |
| `month`   | `2001-08-01 00:00:00.000` |
| `quarter` | `2001-07-01 00:00:00.000` |
| `year`    | `2001-01-01 00:00:00.000` |

上面的示例使用时间戳`2001-08-22 03:04:05.321`作为输入。

**date\_trunc(unit, x)** -> \[与输入相同]

返回`x`截断至`unit`后的值。

## 间隔函数

该部分中的函数支持以下间隔单位：

| 单位          | 说明 |
| :------------ | :--- |
| `millisecond` | 毫秒 |
| `second`      | 秒   |
| `minute`      | 分钟 |
| `hour`        | 小时 |
| `day`         | 天   |
| `week`        | 周   |
| `month`       | 月   |
| `quarter`     | 季度 |
| `year`        | 年   |

**date\_add(unit, value, timestamp)** -> \[与输入相同]

向`timestamp`添加类型为`unit`的间隔`value`。可以使用负值做减法。

**date\_diff(unit, timestamp1, timestamp2)** -> bigint

以`unit`为单位返回`timestamp2 - timestamp1`的值。

## 持续时间函数

`parse_duration`函数支持以下单位：

| 单位 | 说明 |
| :--- | :--- |
| `ns` | 纳秒 |
| `us` | 微秒 |
| `ms` | 毫秒 |
| `s`  | 秒   |
| `m`  | 分钟 |
| `h`  | 小时 |
| `d`  | 天   |

**parse\_duration(string)** -> interval

将格式为`value unit`的`string`解析为一个间隔，其中`value`是以`unit`为单位的小数值。

    SELECT parse_duration('42.8ms'); -- 0 00:00:00.043
    SELECT parse_duration('3.81 d'); -- 3 19:26:24.000
    SELECT parse_duration('5m');     -- 0 00:05:00.000

## MySQL日期函数

该部分中的函数使用与MySQL `date_parse`和`str_to_date`函数兼容的格式字符串。下表根据MySQL手册说明了格式说明符：

| 说明符| 说明|
|----------|----------|
| `%a`| 工作日简称(`Sun` ... `Sat`)|
| `%b`| 月份简称 (`Jan` ... `Dec`)|
| `%c`| 以数字表示的月份 (`1` ... `12`) [4]|
| `%D`| 带英文后缀的一个月中的第几日（`0th`、`1st`、`2nd`、`3rd`、...）|
| `%d`| 以数字表示的一个月中的第几日(`01` ... `31`) [4]|
| `%e`| 以数字表示的一个月中的第几日(`1` ... `31`) [4]|
| `%f`| 微秒（打印6位：`000000` ... `999000`；解析1–9位：`0` ... `999999999`）[1]|
| `%H`| 小时(`00` ... `23`)|
| `%h`| 小时(`01` ... `12`)|
| `%I`| 小时(`01` ... `12`)|
| `%i`| 以数字表示的分钟(`00` ... `59`)|
| `%j`| 一年中的某日(`001` ... `366`)|
| `%k`| 小时(`0` ... `23`)|
| `%l`| 小时(`1` ... `12`)|
| `%M`| 月份名称(`January` ... `December`)|
| `%m`| 以数字表示的月份(`01` ... `12`) [4]|
| `%p`| `AM`或`PM`|
| `%r`| 12小时制时间（`hh:mm:ss`，后跟`AM`或`PM`）|
| `%S`| 秒(`00` ... `59`)|
| `%s`| 秒(`00` ... `59`)|
| `%T`| 24小时制时间(`hh:mm:ss`)|
| `%U`| 周(`00` ... `53`)，其中星期日为一周中的第一天|
| `%u`| 周(`00` ... `53`)，其中星期一为一周中的第一天|
| `%V`| 周(`01` ... `53`)，其中星期日为一周中的第一天；与`%X`配合使用|
| `%v`| 周(`01` ... `53`)，其中星期一为一周中的第一天；与`%x`配合使用|
| `%W`| 周日名称(`Sunday` ... `Saturday`)|
| `%w`| 星期几(`0` ... `6`)，其中星期日是一周中的第一天 [3]|
| `%X`| 周所在的年份，其中星期日是一周中的第一天，以四位数字表示，与`%V`配合使用|
| `%x`| 周所在的年份，其中星期一是一周中的第一天，以四位数字表示，与`%v`配合使用|
| `%Y`| 年份，以四位数字表示|
| `%y`| 年份，以两位数字表示 [2]|
| `%%`| `%`字符|
| `%x`| `x`，用于上面未列出的任何`x`|

| [1] | 时间戳被截断至毫秒。 |
| ------------ | -------------------- |
|              |                      |

| [2] | 在进行解析时，两位数的年份格式采用的范围为`1970`至`2069`，因此“70”将产生年份`1970`，但“69”将产生`2069`。 |
| ------------ | ------------------------------------------------------------ |
|              |                                                              |

| [3] | 尚不支持该说明符。考虑使用[`day_of_week`](#day-of-week)（该函数使用`1-7`，而不使用`0-6`）。 |
| ------------ | ------------------------------------------------------------ |
|              |                                                              |

| [4] | *([1], [2], [3], [4])* 该说明符不支持使用`0`作为月份或日。 |
| ---- | ------------------------------------------------------------ |
|      |                                                              |

**警告**

当前不支持以下说明符：`%D %U %u %V %w %X`

**date\_format(timestamp, format)** -> varchar

使用`format`将`timestamp`格式化为一个字符串。

**date\_parse(string, format)** -> timestamp

使用`format`将`string`解析为一个时间戳。

## Java日期函数

该部分中的函数使用与Joda-Time的[DateTimeFormat](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html)模式格式兼容的格式字符串。

**format\_datetime(timestamp, format)** -> varchar

使用`format`将`timestamp`格式化为一个字符串。

**parse\_datetime(string, format)** -> timestamp with time zone

使用`format`将`string`格式化为一个timestamp with time zone。

## 提取函数

`extract`函数支持以下字段：

| 字段| 说明|
|:----------|:----------|
| `YEAR`            | [`year()`](#year) |
| `QUARTER`         | [`quarter()`](#quarter) |
| `MONTH`           | [`month()`](#month) |
| `WEEK`            | [`week()`](#week) |
| `DAY`             | [`day()`](#day) |
| `DAY_OF_MONTH`    | [`day()`](#day) |
| `DAY_OF_WEEK`     | [`day_of_week()`](#day-of-week) |
| `DOW`             | [`day_of_week()`](#day-of-week) |
| `DAY_OF_YEAR`     | [`day_of_year()`](#day-of-year) |
| `DOY`             | [`day_of_year()`](#day-of-year) |
| `YEAR_OF_WEEK`    | [`year_of_week()`](#year-of-week) |
| `YOW`             | [`year_of_week()`](#year-of-week) |
| `HOUR`            | [`hour()`](#hour) |
| `MINUTE`          | [`minute()`](#minute) |
| `SECOND`          | [`second()`](#second) |
| `TIMEZONE_HOUR`   | [`timezone_hour()`](#timezone-hour) |
| `TIMEZONE_MINUTE` | [`timezone_minute()`](#timezone-minute) |

`extract`函数支持的类型因要提取的字段而异。大多数字段都支持所有日期和时间类型。

**extract(field FROM x)** -> bigint

从`x`返回`field`。

**注意**

该SQL标准函数使用特殊的语法来指定参数。

## 便捷提取函数

#### DAY

**day(x)** -> bigint

从`x`返回一个月中的第几日。

#### DAY OF MONTH

**day\_of\_month(x)** -> bigint

这是`day`的别名。

#### DAY OF WEEK

**day\_of\_week(x)** -> bigint

从`x`返回星期几(ISO)。值的范围为`1`（星期一）至`7`（星期日）。

#### DAY OF YEAR

**day\_of\_year(x)** -> bigint

从`x`返回一年中的第几日。值的范围为`1`至`366`。

#### DOW

**dow(x)** -> bigint

这是`day_of_week`的别名。

#### DOY

**doy(x)** -> bigint

这是`day_of_year`的别名。

#### HOUR

**hour(x)** -> bigint

从`x`返回一天中的第几个小时。值的范围为`0`至`23`。

#### MILLISECOND

**millisecond(x)** -> bigint

从`x`返回一秒中的第几个毫秒。

#### MINUTE

**minute(x)** -> bigint

从`x`返回一小时中的第几分钟。

#### MONTH

**month(x)** -> bigint

从`x`返回一年中的某个月份。

#### QUARTER

**quarter(x)** -> bigint

从`x`返回一年中的某个季度。值的范围为`1`至`4`。

#### SECOND

**second(x)** -> bigint

从`x`返回一分中的第几秒。

#### TIMEZONE HOURS

**timezone\_hour(timestamp)** -> bigint

从`timestamp`返回时区偏移量的小时数。

#### TIMEZONE MINUTE

**timezone\_minute(timestamp)** -> bigint

从`timestamp`返回时区偏移量的分钟数。

#### WEEK

**week(x)** -> bigint

从`x`返回一年中的[第几周(ISO)](https://calendars.wikia.org/wiki/ISO_week_date)。值的范围为`1`至`53`。

#### WEEK OF YEAR

**week\_of\_year(x)** -> bigint

这是`week`的别名。

#### YEAR

**year(x)** -> bigint

从`x`返回年份。

#### YEAR OF WEEK

**year\_of\_week(x)** -> bigint

从`x`返回[ISO周](https://calendars.wikia.org/wiki/ISO_week_date)的年份。

#### YOW

**yow(x)** -> bigint

这是`year_of_week`的别名。

[^1]: 该说明符不支持使用`0`作为月或日。

[^2]: 该说明符不支持使用`0`作为月或日。

[^3]: 该说明符不支持使用`0`作为月或日。

[^4]: 时间戳被截断至毫秒。

[^5]: 该说明符不支持使用`0`作为月或日。

[^6]: 尚不支持该说明符。考虑使用`day_of_week`（该函数使用`1-7`，而不使用`0-6`）。

[^7]: 在进行解析时，两位数的年份格式采用的范围为`1970`至`2069`，因此“70”将产生年份`1970`，但“69”将产生`2069`。