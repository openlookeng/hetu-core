
# 字符串函数和运算符

## 字符串运算符

`||`运算符执行连接。

## 字符串函数

**注意**

这些函数假定输入字符串包含有效的UTF-8编码的Unicode代码点。不会显式检查UTF-8数据是否有效，对于无效的UTF-8数据，函数可能会返回错误的结果。可以使用`from_utf8`来更正无效的UTF-8数据。

此外，这些函数对Unicode代码点进行运算，而不是对用户可见的*字符*（或*字形群集*）进行运算。某些语言将多个代码点组合成单个用户感观字符（这是语言书写系统的基本单位），但是函数会将每个代码点视为单独的单位。

`lower`和`upper`函数不执行某些语言所需的区域设置相关、上下文相关或一对多映射。

具体而言，对于立陶宛语、土耳其语和阿塞拜疆语，这将返回不正确的结果。

**chr(n)** -> varchar

以单个字符串的形式返回Unicode代码点`n`。

**codepoint(string)** -> integer

返回`string`的唯一字符的Unicode编码点。

**concat(string1, ..., stringN)** -> varchar

返回`string1`、`string2`、`...`、`stringN`的连接结果。该函数提供与 SQL 标准连接运算符(`||`)相同的功能。

**hamming\_distance(string1, string2)** -> bigint

返回`string1`和`string2`的汉明距离，即对应字符不同的位置的数量。

请注意，这两个字符串的长度必须相同。

**length(string)** -> bigint

以字符为单位返回`string`的长度。

**levenshtein\_distance(string1, string2)** -> bigint

返回`string1`和`string2`的Levenshtein编辑距离，即将`string1`更改为`string2`所需的最小单字符编辑（插入、删除或替换）数量。

**lower(string)** -> varchar

将`string`转换为小写。

**lpad(string, size, padstring)** -> varchar

使用`padstring`将`string`左填充至`size`个字符。如果`size`小于`string`的长度，则结果被截断为`size`个字符。`size`不得为负数，并且`padstring`必须为非空值。

**ltrim(string)** -> varchar

删除`string`中的前导空格。

**replace(string, search)** -> varchar

删除`string`中的所有`search`实例。

**replace(string, search, replace)** -> varchar

将`string`中`search`的所有实例替换为`replace`。

**reverse(string)** -> varchar

以相反的字符顺序返回`string`。

**rpad(string, size, padstring)** -> varchar

使用`padstring`将`string`右填充至`size`个字符。如果`size`小于`string`的长度，则结果被截断为`size`个字符。`size`不得为负数，并且`padstring`必须为非空值。

**rtrim(string)** -> varchar

删除`string`中的尾随空格。

**split(string, delimiter)** -> array(varchar)

根据`delimiter`拆分`string`并返回一个数组。

**split(string, delimiter, limit)** -> array(varchar)

按`delimiter`拆分`string`并返回一个大小最大为`limit`的数组。该数组中的最后一个元素始终包含`string`中的所有剩余内容。`limit`必须为正数。

**split\_part(string, delimiter, index)** -> varchar

按`delimiter`拆分`string`并返回字段`index`。字段索引从`1`开始。如果索引大于字段数，则返回NULL。

**split\_to**\_map(string, entryDelimiter, keyValueDelimiter) -> map\<varchar, varchar>

按`entryDelimiter`和`keyValueDelimiter`拆分`string`并返回Map。`entryDelimiter`将`string`拆分成键值对。

`keyValueDelimiter`将每个键值对拆分成键和值。

**split\_to**\_multimap(string, entryDelimiter, keyValueDelimiter) -> map(varchar, array(varchar))

按`entryDelimiter`和`keyValueDelimiter`拆分`string`并返回包含每个唯一键的值数组的Map。

`entryDelimiter`将`string`拆分成键值对。  `keyValueDelimiter`将每个键值对拆分成键和值。每个键的值的顺序与其出现在`string`中的顺序相同。

**strpos(string, substring)** -> bigint

返回`string`中第一个`substring`实例的起始位置。位置从`1`开始。如果未找到，则返回`0`。

**position(substring IN string)** -> bigint

返回`string`中第一个`substring`实例的起始位置。位置从`1`开始。如果未找到，则返回`0`。

**substr(string, start)** -> varchar

返回从起始位置`start`开始的`string`其余部分。位置从`1`开始。负起始位置表示相对于字符串的末尾。

**substr(string, start, length)** -> varchar

返回从起始位置`start`开始且长度为`length`的`string`子字符串。位置从`1`开始。负起始位置表示相对于字符串的末尾。

**trim(string)** -> varchar

删除`string`中的前导和尾随空格。

**upper(string)** -> varchar

将`string`转换为大写。

**word\_stem(word)** -> varchar

返回`word`的英语词干。

**word\_stem(word, lang)** -> varchar

返回`word`的`lang`语词干。

## Unicode函数

**normalize(string)** -> varchar

使用NFC范式对`string`进行转换。

**normalize(string, form)** -> varchar

使用指定的范式对`string`进行转换。`form`必须为以下关键字之一：

| 范式| 说明| 
|:----------|:----------| 
| `NFD`| 规范分解| 
| `NFC`| 规范分解，后跟规范合成| 
| `NFKD`| 兼容性分解| 
| `NFKC`| 兼容性分解，后跟规范合成| 

**注意**

该SQL标准函数具有特殊的语法，要求指定`form`为关键字，而不是字符串。

**to\_utf8(string)** -> varbinary

将`string`编码为UTF-8 varbinary表示形式。

**from\_utf8(binary)** -> varchar

从`binary`解码UTF-8编码字符串。使用Unicode替换字符`U+FFFD`替换无效的UTF-8序列。

**from\_utf8(binary, replace)** -> varchar

从`binary`解码UTF-8编码字符串。使用`replace`替换无效的UTF-8序列。替换字符串`replace`必须是单个字符或者为空（此时删除无效字符）。