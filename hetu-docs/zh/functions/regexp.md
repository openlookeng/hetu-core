
# 正则表达式函数

所有正则表达式函数都使用[Java模式](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html)语法，但有一些值得注意的例外：

- 当使用多行模式（通过`(?m)`标志启用）时，仅将`\n`识别为行终止符。此外，不支持`(?d)`标志，不得使用该标志。
- 不区分大小写的匹配（通过`(?i)`标志启用）始终以支持Unicode的方式执行。不过，不支持上下文相关和局部相关的匹配。此外，不支持`(?u)`标志，不得使用该标志。
- 不支持代理项对。例如，`\uD800\uDC00`不被视为`U+10000`，必须将其指定为`\x{10000}`。对于没有基字符的不占位标记，会错误地处理边界(`\b`)。
- 在字符类（如`[A-Z123]`）中不支持`\Q`和`\E`，而应将其视为字面量。
- 支持Unicode字符类(`\p{prop}`)，但有如下差异：
  - 必须删除名称中的所有下划线。例如，使用`OldItalic`代替`Old_Italic`。
  - 必须直接指定脚本，而不使用`Is`、`script=`或`sc=`前缀。示例：`\p{Hiragana}`
  - 必须使用`In`前缀指定块。不支持`block=`和`blk=`前缀。示例：`\p{Mongolian}`
  - 必须直接指定类别，而不使用`Is`、`general_category=`或`gc=`前缀。示例：`\p{L}`
  - 必须直接指定二进制属性，而不使用`Is`。示例：`\p{NoncharacterCodePoint}`

**regexp\_extract\_all(string, pattern)** -> array(varchar)

返回正则表达式`pattern`在`string`中匹配的子字符串：

    SELECT regexp_extract_all('1a 2b 14m', '\d+'); -- [1, 2, 14]

**regexp\_extract\_all(string, pattern, group)** -> array(varchar)

查找`string`中出现的所有正则表达式`pattern`实例并返回[捕获组编号](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html#gnumber)`group`：

    SELECT regexp_extract_all('1a 2b 14m', '(\d+)([a-z]+)', 2); -- ['a', 'b', 'm']

**regexp\_extract(string, pattern)** -> varchar

返回正则表达式`pattern`在`string`中匹配的第一个子字符串：

    SELECT regexp_extract('1a 2b 14m', '\d+'); -- 1

**regexp\_extract(string, pattern, group)** -> varchar

查找`string`中出现的第一个正则表达式`pattern`实例并返回[捕获组编号](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html#gnumber)`group`：

    SELECT regexp_extract('1a 2b 14m', '(\d+)([a-z]+)', 2); -- 'a'

**regexp\_like(string, pattern)** -> boolean

计算正则表达式`pattern`并确定它是否包含在`string`中。

该函数与`LIKE`运算符类似，不过只需在`string`中包含模式，而无需匹配整个`string`。换句话说，该函数执行的是*包含*运算，而不是*匹配*运算。可以通过使用`^`和`$`定位模式来匹配整个字符串：

    SELECT regexp_like('1a 2b 14m', '\d+b'); -- true

**regexp\_replace(string, pattern)** -> varchar

从`string`中删除由正则表达式`pattern`匹配的子字符串的每个实例：

    SELECT regexp_replace('1a 2b 14m', '\d+[ab] '); -- '14m'

**regexp\_replace(string, pattern, replacement)** -> varchar

将`string`中由正则表达式`pattern`匹配的子字符串的每个实例替换为`replacement`。可以使用`$g`（对于编号的组）或`${name}`（对于命名的组）在`replacement`中引用[捕获组](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html#gnumber)。替换时，可以通过使用反斜杠(`\$`)进行转义来包含美元符号(`$`)：

    SELECT regexp_replace('1a 2b 14m', '(\d+)([ab]) ', '3c$2 '); -- '3ca 3cb 14m'

**regexp\_replace(string, pattern, function)** -> varchar

使用`function`替换`string`中由正则表达式`pattern`匹配的子字符串的每个实例。会针对每个匹配项调用`lambda expression <lambda>` `function`，其中以数组形式传入[捕获组](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html#cg)。捕获组编号从1开始；整个匹配没有组（如果需要，可以使用圆括号将整个表达式括起来）：

    SELECT regexp_replace('new york', '(\w)(\w*)', x -> upper(x[1]) || lower(x[2])); --'New York'

**regexp\_split(string, pattern)** -> array(varchar)

使用正则表达式`pattern`拆分`string`并返回一个数组。保留尾随空字符串：

    SELECT regexp_split('1a 2b 14m', '\s*[a-z]+\s*'); -- [1, 2, 14, ]