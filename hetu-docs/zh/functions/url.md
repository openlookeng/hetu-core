
# URL函数

## 提取函数

URL提取函数从HTTP URL（或任何符合[2396](https://tools.ietf.org/html/rfc2396.html)标准的有效URI）中提取组成部分。支持以下语法：

```
[protocol:][//host[:port]][path][?query][#fragment]
```

提取的组成部分不包含`:`或`?`等URI语法分隔符。

**url\_extract\_fragment(url)** -> varchar

从`url`返回片断标识符。

**url\_extract\_host(url)** -> varchar

从`url`返回主机。

**url\_extract\_parameter(url, name** -> varchar

从`url`返回第一个名为`name`的查询字符串参数的值。按照[1866#section-8.2.1](https://tools.ietf.org/html/rfc1866.html#section-8.2.1)中指定的典型方式来处理参数提取。

**url\_extract\_path(url)** -> varchar

从`url`返回路径。

**url\_extract\_port(url)** -> bigint

从`url`返回端口号。

**url\_extract\_protocol(url)** -> varchar

从`url`返回协议。

**url\_extract\_query(url)** -> varchar

从`url`返回查询字符串。

## 编码函数

**url\_encode(value)** -> varchar

通过对`value`进行编码来对其进行转义，以便可以安全地将其包含在URL查询参数名称和值中：

- 不对字母数字字符进行编码。
- 不对字符`.`、`-`、`*`和`_`进行编码。
- 将ASCII空格字符编码为`+`。
- 将所有其他字符都转换为UTF-8，将字节编码为字符串`%XX`，其中`XX`是UTF-8字节的大写十六进制值。

**url\_decode(value)** -> varchar

对URL编码`value`进行反转义。该函数是`url_encode`的反函数。