
数据类型
==========

openLooKeng有一组内置的数据类型，如下所述。可能通过插件提供更多的类型。


**注意**

*连接器不需要支持所有的数据类型。连接器支持的数据类型，请参见[连接器](../connector/_index.md)文档。*

布尔类型
-------

### `BOOLEAN`

> 捕获布尔值`true`和`false`。

整数类型
-------

### `TINYINT`

> 一个8比特带符号的2补码整数，最小值为`-2^7`，最大值为`2^7 - 1`。

### `SMALLINT`

> 一个16比特带符号的2补码整数，最小值为`-2^15`，最大值为`2^15 - 1`。

### `INTEGER`

> 一个32比特带符号的2补码整数，最小值为`-2^31`，最大值为`2^31 - 1`。该类型也被称为`INT`。

### `BIGINT`

> 一个64比特带符号的2补码整数，最小值为`-2^63`，最大值为`2^63 - 1`。

浮点类型
--------------

### `REAL`

> 一种不精确的可变精度的32比特的数据类型，实现IEEE 754标准定义的二进制浮点运算。
>
> 此类型也被称为`FLOAT`。
>
> 例如: REAL '10.3', REAL '10.3e0', REAL '1.03e1'

### `DOUBLE`

> 一种不精确的可变精度的64比特的数据类型，它实现了IEEE 754标准定义的二进制浮点运算。
>
> 该类型也被称为`DOUBLE PRECISION`。

固定精度类型
---------------

### `DECIMAL`

> 固定精度的十进制数。精度最高可达38位，但性能最高可达18位。
>
>该类型也被称为`NUMERIC`和`DEC`。
>
> 此数据类型有两个参数：
>
> -   **precision** - 表示总位数。
> -   **scale** - 小数位数。Scale可选，默认为0。
>
> 类型定义示例：`DECIMAL(10,3)`, `DECIMAL(20)`
>
> 文本示例：`DECIMAL '10.3'`, `DECIMAL '1234567890'`, `1.1`
>
>
> **注意**
>
>由于兼容性原因，没有显式类型说明符的十进制文字（例如 `1.2`）默认作为`DOUBLE`类型的值处理。但在以后的版本中也可能会有变化。此行为受以下控制：

>
> -   系统属性: `parse-decimal-literals-as-double`
> -   会话属性: `parse_decimal_literals_as_double`

字符串类型
------

### `VARCHAR`

> 可变长度字符数据，最大长度可选。
>
> 该类型也被称为`STRING`。请注意，`STRING`定义的是一个无限长的字符数据，您不能指定长度，否则它就成为`VARCHAR(length)`类型了。
>
> 类型定义示例：`varchar`, `varchar(20)`, `string`
>
> SQL语句支持简单文字和Unicode使用：
>
> -   文本字符串: `'Hello winter !'`
> -   含默认转义字符的Unicode字符串：`U&'Hello winter \2603 !'`
> -   含自定义转义字符的Unicode字符串：`U&'Hello winter #2603 !'UESCAPE '#'`
>
> Unicode字符串以`U&`为前缀，任何4位Unicode字符前都需要转义符。
> 在上例中，`\2603`和`#2603`代表雪人符号。
> 对于6位的Unicode长编码，需要在代码前使用一个加号。例如，您需要在笑脸表情前使用`\+01F600`。


### `CHAR`

> 固定长度字符。对于不指定长度的`CHAR`类型，默认长度为1。一个`CHAR(x)`包含`x`个字符。例如，将`dog`转换为`CHAR(7)`会增加4个隐式尾部空格。在比较`CHAR`值时包括前导和尾部空格。因此，两个不同长度的字符值（`CHAR(x）`和`CHAR(y)`，其中`x != y`)永远不会相等。

>
> 类型定义示例：`char`, `char(20)`

### `VARBINARY`

> 可变长度二进制数据。
>
> 
> **注意**
> 
> 目前仍不支持带长度的二进制字符：`varbinary(n)`
>

### `JSON`

> JSON数值类型，可以是JSON对象、JSON数组、JSON数字、JSON字符串、`true`、`false`或`null`。

日期和时间类型
-------------

### `DATE`

> 日历日期（年、月、日）
>
> 例如: `DATE '2001-08-22'`

### `TIME`

> 不带时区的时间（时、分、秒、毫秒）此类型的值将在会话时区中解析和呈现。
>
> 例如: `TIME '01:02:03.456'`

### `TIME WITH TIME ZONE`

> 带时区的时间（时、分、秒、毫秒）。此类型的值将使用该值中的时区进行呈现。
>
> 例如: `TIME '01:02:03.456 America/Los_Angeles'`

### `TIMESTAMP`

> 包含日期和时间的即时时间，不包含时区。此类型的值将在会话时区中解析和呈现。
>
> 例如: `TIMESTAMP '2001-08-22 03:04:05.321'`

### `TIMESTAMP WITH TIME ZONE`

> 即时时间，包括日期、时间和时区。此类型的值将使用该值中的时区进行呈现。
>
> 例如: `TIMESTAMP '2001-08-22 03:04:05.321 America/Los_Angeles'`

### `INTERVAL YEAR TO MONTH`

> 年和月的跨度。
>
> 例如: `INTERVAL '3' MONTH`

### `INTERVAL DAY TO SECOND`

> 天数、小时、分钟、秒和毫秒的跨度。
>
> 例如: `INTERVAL '2' DAY`

结构类型
----------

### `ARRAY`

> 指定组件类型的数组。
>
> 例如: `ARRAY[1, 2, 3]`

### `MAP`

> 指定组件类型之间的映射。
>
> 例如: `MAP(ARRAY['foo', 'bar'], ARRAY[1, 2])`

### `ROW`

> 由混合类型的字段组成的结构。字段可以是任何SQL类型。
>
> 行字段默认不命名，但可以指定名称。
>
> 例如: `CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))`
>
> 已命名的行字段通过字段引用运算符`.`访问。
>
> 例如: `CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE)).x`
>
> 已命名或未命名的行字段通过下标运算符`[]`按位置访问。位置从`1`开始且必须是常量。
>
> 例如: `ROW(1, 2.0)[1]`

网络地址
---------------

### `IPADDRESS`

> 可表示IPv4地址或IPv6地址。对内为纯IPv6地址。可通过*IPv4-mapped IPv6 address* range ([RFC 4291#section-2.5.5.2](https://tools.ietf.org/html/rfc4291.html#section-2.5.5.2 ))来支持对IPv4地址的处理。在创建`IPADDRESS`时，IPv4地址将映射到指定的范围。格式化`IPADDRESS`时，在映射范围内的任何地址都会被格式化为IPv4地址。其他地址将使用[RFC 5952](https://tools.ietf.org/html/rfc5952.html) 中定义的规范格式格式化为IPv6地址。
>
> 例如: `IPADDRESS '10.0.0.1'`, `IPADDRESS '2001:db8::1'`

UUID
----

### `UUID`

> 此类型表示UUID（通用唯一标识符），也称为GUID（全局唯一标识符），使用[RFC 4122](https://tools.ietf.org/html/rfc4122.html) 中定义的格式。
>
> 例如: `UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'`

HyperLogLog
-----------

计算近似的非重复计数比使用[HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) 数据草图进行精确计数成本低得多。
请参见 [HyperLogLog 函数](../functions/hyperloglog.md)。

### `HyperLogLog`

> HyperLogLog 草图可高效地计算 [approx_distinct()](../functions/aggregate.md)。它开始时是稀疏表示，当效率提高时，就切换到密集表示。

### `P4HyperLogLog`

> P4HyperLogLog草图类似于`hyperloglog_type`，但它从始至终都采用密集表示形式。

分位点摘要
---------------

### `QDigest`

> 分位点摘要（qdigest）是一种摘要结构，它捕捉指定输入集的数据的近似分布，并且可以通过查询从分布中检索近似分位点值。
> qdigest的准确程度是可调的，使更精确的结果会占用更多空间。
>
> 对于在某一分位数处属于什么值的查询，可用qdigest提供近似回答。qdigest的一个有用的特性是它们是可加的，这意味着它们可以合并在一起而不损失精度。
>
> 当`approx_percentile`的部分结果可以重用时，qdigest就会发挥更大作用。例如，人们可能对在一周内每天读取的第99百分位数值感兴趣。
这种情况下，与其使用`approx_percentile`计算过去一周的数据，不如使用`qdigest`。`qdigest`可以每天存储，并快速合并以检索第99个百分位值。
