
# 转换函数

如果可能的话，openLooKeng 会隐式地将数字和字符值转换为正确的类型。对于任何其他类型，默认情况下，openLooKeng 不会隐式地进行转换。例如，期望返回 varchar 值的查询不会自动将 bigint 值转换为等效的 varchar 值。

必要时，可以显式地将值转换为特定的类型。

或者，您可以启用 `implicit conversion` 功能，然后 openLooKeng 将尝试自动应用源类型和目标类型之间的转换。

## 转换函数

**cast(value AS type)** -> type

将值显式转换为某个类型。可以使用该函数将 varchar 转换为数值类型，反之亦然。

**try\_cast(value AS type)** -> type

与 `cast` 类似，如果转换失败，则返回 NULL。

## 格式化

**format(format, args...)** -> varchar

使用指定的[格式字符串](https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html#syntax)和参数返回一个格式化字符串：

    SELECT format('%s%%', 123); -- '123%'
    SELECT format('%.5f', pi()); -- '3.14159'
    SELECT format('%03d', 8); -- '008'
    SELECT format('%,.2f', 1234567.89); -- '1,234,567.89'
    SELECT format('%-7s,%7s', 'hello', 'world'); --  'hello  ,  world'
    SELECT format('%2$s %3$s %1$s', 'a', 'b', 'c'); -- 'b c a'
    SELECT format('%1$tA, %1$tB %1$te, %1$tY', date '2006-07-04'); -- 'Tuesday, July 4, 2006'

## 数据大小

`parse_presto_data_size` 函数支持以下单位：

| 单位 | 说明   | 值     |
| :--- | :----- | :----- |
| `B`  | 字节   | 1      |
| `kB` | 千字节 | 1024   |
| `MB` | 兆字节 | 1024^2 |
| `GB` | 吉字节 | 1024^3 |
| `TB` | 太字节 | 1024^4 |
| `PB` | 拍字节 | 1024^5 |
| `EB` | 艾字节 | 1024^6 |
| `ZB` | 泽字节 | 1024^7 |
| `YB` | 尧字节 | 1024^8 |

**parse\_presto\_data\_size(string)** -> decimal(38)

将格式为 `value unit` 的 `string` 解析为一个数字，其中 `value` 是 `unit` 值的小数表示形式：

    SELECT parse_presto_data_size('1B'); -- 1
    SELECT parse_presto_data_size('1kB'); -- 1024
    SELECT parse_presto_data_size('1MB'); -- 1048576
    SELECT parse_presto_data_size('2.3MB'); -- 2411724

## 隐式转换

您可以通过设置会话属性来启用隐式转换。

    SET SESSION implicit_conversion=true

默认情况下，属性值为 false，此时隐式转换关闭。

如果属性值设置为 true，每当 openLooKeng 发现类型不匹配，但可能彼此兼容时，openLooKeng 就会通过应用 `CAST` 函数来自动重写语句。

这样，用户就无需显式地添加 `CAST` 函数来对类型进行转换。

例如，期望返回 varchar 值的查询会自动将 bigint 值转换为等效的 varchar 值。

很显然，并非所有的数据类型都是相互兼容的，下表列出了所有基本数据类型的所有可行转换。

|           | BOOLEAN | TINYINT | SMALLINT | INTEGER | BIGINT | REAL | DOUBLE | DECIMAL | VARCHAR | CHAR | VARBINARY | JSON | DATE  | TIME  | TIME WITH TIME ZONE | TIMESTAMP | TIMESTAMP WITH TIME ZONE |
| --------- | ------- | ------- | -------- | ------- | ------ | ---- | ------ | ------- | ------- | ---- | --------- | ---- | ----- | ----- | ------------------- | --------- | ------------------------ |
| BOOLEAN   |         | Y(1)    | Y        | Y       | Y      | Y    | Y      | Y       | Y(2)    | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| TINYINT   | Y(3)    |         | Y        | Y       | Y      | Y    | Y      | Y       | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| SMALLINT  | Y       | Y(4)    |          | Y       | Y      | Y    | Y      | Y       | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| INTEGER   | Y       | Y       | Y        |         | Y      | Y    | Y      | Y       | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| BIGINT    | Y       | Y       | Y        | Y       |        | Y    | Y      | Y       | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| REAL      | Y       | Y       | Y        | Y       | Y      |      | Y      | Y(5)    | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| DOUBLE    | Y       | Y       | Y        | Y       | Y      | Y    |        | Y       | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| DECIMAL   | Y       | Y       | Y        | Y       | Y      | Y    | Y      | (6)     | Y       | N    | N         | Y    | N     | N     | N                   | N         | N                        |
| VARCHAR   | Y(7)    | Y       | Y        | Y       | Y      | Y    | Y      | Y(8)    |         | Y(9) | Y         | Y    | Y(10) | Y(11) | Y(12)               | Y(13)     | Y                        |
| CHAR      | N       | N       | N        | N       | N      | N    | N      | N       | Y       |      | N         | N    | N     | N     | N                   | N         | N                        |
| VARBINARY | N       | N       | N        | N       | N      | N    | N      | N       | N       | N    |           | N    | N     | N     | N                   | N         | N                        |
| JSON      | N       | N       | N        | N       | N      | N    | N      | N       | Y       | N    | N         |      | N     | N     | N                   | N         | N                        |
| DATE      | N       | N       | N        | N       | N      | N    | N      | N       | Y       | N    | N         | Y    |       | N     | N                   | Y(14)     | Y                        |
| TIME      | N       | N       | N        | N       | N      | N    | N      | N       | Y       | N    | N         | N    | N     |       | Y(15)               | Y(16)     | Y                        |
| TIME WITH | N       | N       | N        | N       | N      | N    | N      | N       | Y       | N    | N         | N    | N     | Y     |                     | Y         | Y                        |
| TIME ZONE |         |         |          |         |        |      |        |         |         |      |           |      |       |       |                     |           |                          |
| TIMESTAMP | N       | N       | N        | N       | N      | N    | N      | N       | Y       | N    | N         | N    | Y     | Y     | Y                   |           | Y                        |
| TIMESTAMP | N       | N       | N        | N       | N      | N    | N      | N       | Y       | N    | N         | N    | Y     | Y     | Y                   | Y         |                          |
| WITH TIME |         |         |          |         |        |      |        |         |         |      |           |      |       |       |                     |           |                          |
| ZONE      |         |         |          |         |        |      |        |         |         |      |           |      |       |       |                     |           |                          |

 

**说明：**

- Y 或 Y(#)：表示支持隐式转换。但您可能需要注意一些限制。请参阅以下各项。
- N：表示不支持隐式转换

(1)BOOLEAN -> NUMBER：转换结果只能为 0 或 1。

(2)BOOLEAN -> VARCHAR：转换结果只能为 TRUE 或 FALSE。

(3)NUMBER -> BOOLEAN：0 将转换为 false，其他值将转换为 true。

(4)BIG PRECISION -> SAMLL：当数据超出 SMALL 范围时，转换将失败。

(5)REAL/FLOAT -> DECIMAL：当数据超出 DECIMAL 范围时，转换将失败。当超出范围时，范围将被截断。

(6)DECIMAL -> DECIMAL：当数据超出 DECIMAL 范围时，转换将失败。当超出范围时，范围将被截断。

(7)VARCHAR -> BOOLEAN：只能转换“0”、“1”、“TRUE”和“FALSE”。其他值的转换会失败。

(8)VARCHAR -> DECIMAL：当数据不是数值或转换后的值超出 DECIMAL 范围时，转换将失败。当超出范围时，范围将被截断。

(9)VARCHAR -> CHAR：如果 VARCHAR 的长度大于 CHAR，它将被截断。

(10)VARCHAR -> DATE：只能将 VARCHAR 格式化为“YYYY-MM-DD”的形式，例如 2000-01-01。

(11)VARCHAR -> TIME：只能将 VARCHAR 格式化为“HH:MM:SS.XXX”的形式。

(12)VARCHAR -> TIME ZONE：只能将 VARCHAR 格式化为“HH:MM:SS.XXX XXX”的形式，例如 01:02:03.456 America/Los\_Angeles

(13)VARCHAR -> TIMESTAMP：只能将 VARCHAR 格式化为“YYYY-MM-DD HH:MM:SS.XXX”的形式。

(14)DATE -> TIMESTAMP：会自动使用 0 填充时间，例如将 2010-01-01 转换为 2010-01-01 00:00:00.000。

(15)TIME -> TIME WITH TIME ZONE：会自动填充默认时区。

(16)TIME -> TIMESTAMP：会自动添加默认日期 1970-01-01。

## 其他

**typeof(expr)** -> varchar

返回所提供的表达式类型的名称：

    SELECT typeof(123); -- integer
    SELECT typeof('cat'); -- varchar(3)
    SELECT typeof(cos(2) + 1.5); -- double