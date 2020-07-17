
# JSON函数和运算符

## 转换为JSON

支持从`BOOLEAN`、`TINYINT`、`SMALLINT`、`INTEGER`、`BIGINT`、`REAL`、`DOUBLE`或`VARCHAR`进行转换。当数组的元素类型为支持的类型之一、Map的键类型是`VARCHAR`且Map的值类型是支持的类型之一或行的每个字段类型是支持的类型之一时支持从`ARRAY`、`MAP`或`ROW`进行转换。下面通过示例展示了转换的行为：

```
SELECT CAST(NULL AS JSON); -- NULL
SELECT CAST(1 AS JSON); -- JSON '1'
SELECT CAST(9223372036854775807 AS JSON); -- JSON '9223372036854775807'
SELECT CAST('abc' AS JSON); -- JSON '"abc"'
SELECT CAST(true AS JSON); -- JSON 'true'
SELECT CAST(1.234 AS JSON); -- JSON '1.234'
SELECT CAST(ARRAY[1, 23, 456] AS JSON); -- JSON '[1,23,456]'
SELECT CAST(ARRAY[1, NULL, 456] AS JSON); -- JSON '[1,null,456]'
SELECT CAST(ARRAY[ARRAY[1, 23], ARRAY[456]] AS JSON); -- JSON '[[1,23],[456]]'
SELECT CAST(MAP_FROM_ENTRIES(ARRAY[('k1', 1), ('k2', 23), ('k3', 456)]) AS JSON); -- JSON '{"k1":1,"k2":23,"k3":456}'
SELECT CAST(CAST(ROW(123, 'abc', true) AS ROW(v1 BIGINT, v2 VARCHAR, v3 BOOLEAN)) AS JSON
```

**注意**

NULL到`JSON`的转换并不能简单地实现。从独立的`NULL`进行转换将产生一个SQL`NULL`，而不是`JSON 'null'`。不过，在从包含`NULL`的数组或Map进行转换时，生成的`JSON`将包含`null`。

**注意**

在从`ROW`转换为`JSON`时，结果是一个JSON数组，而不是一个JSON对象。这是因为对于SQL中的行，位置比名称更重要。

## 从JSON进行转换

> 支持转换为`BOOLEAN`、`TINYINT`、`SMALLINT`、`INTEGER`、`BIGINT`、`REAL`、`DOUBLE`或`VARCHAR`。当数组的元素类型为支持的类型之一或Map的键类型为`VARCHAR`且Map的值类型为支持的类型之一时，支持转换为`ARRAY`和`MAP`。下面通过示例展示了转换的行为：
> 
>     SELECT CAST(JSON 'null' AS VARCHAR); -- NULL
>     SELECT CAST(JSON '1' AS INTEGER); -- 1
>     SELECT CAST(JSON '9223372036854775807' AS BIGINT); -- 9223372036854775807
>     SELECT CAST(JSON '"abc"' AS VARCHAR); -- abc
>     SELECT CAST(JSON 'true' AS BOOLEAN); -- true
>     SELECT CAST(JSON '1.234' AS DOUBLE); -- 1.234
>     SELECT CAST(JSON '[1,23,456]' AS ARRAY(INTEGER)); -- [1, 23, 456]
>     SELECT CAST(JSON '[1,null,456]' AS ARRAY(INTEGER)); -- [1, NULL, 456]
>     SELECT CAST(JSON '[[1,23],[456]]' AS ARRAY(ARRAY(INTEGER))); -- [[1, 23], [456]]
>     SELECT CAST(JSON '{"k1":1,"k2":23,"k3":456}' AS MAP(VARCHAR, INTEGER)); -- {k1=1, k2=23, k3=456}
>     SELECT CAST(JSON '{"v1":123,"v2":"abc","v3":true}' AS ROW(v1 BIGINT, v2 VARCHAR, v3 BOOLEAN)); -- {v1=123, v2=abc, v3=true}
>     SELECT CAST(JSON '[123,"abc",true]' AS ROW(v1 BIGINT, v2 VARCHAR, v3 BOOLEAN)); -- {value1=123, value2=abc, value3=true}

**注意**

JSON数组可以具有混合元素类型，JSON Map可以有混合值类型。这使得在某些情况下无法将其转换为SQL数组和Map。为了解决该问题，openLooKeng支持对数组和Map进行部分转换：

    SELECT CAST(JSON '[[1, 23], 456]' AS ARRAY(JSON)); -- [JSON '[1,23]', JSON '456']
    SELECT CAST(JSON '{"k1": [1, 23], "k2": 456}' AS MAP(VARCHAR, JSON)); -- {k1 = JSON '[1,23]', k2 = JSON '456'}
    SELECT CAST(JSON '[null]' AS ARRAY(JSON)); -- [JSON 'null']

**注意**

在从`JSON`转换为`ROW`时，支持JSON数组和JSON对象。

## JSON函数

**is\_json\_scalar(json)** -> boolean

确定`json`是否为标量（即JSON数字、JSON字符串、`true`、`false`或`null`）：

    SELECT is_json_scalar('1'); -- true
    SELECT is_json_scalar('[1, 2, 3]'); -- false

**json\_array\_contains(json, value)** -> boolean

确定`json`（包含JSON数组的字符串）中是否存在`value`：

    SELECT json_array_contains('[1, 2, 3]', 2);

**json\_array\_get(json\_array, index)** -> json

**警告**

该函数的语义已被破坏。如果提取的元素是字符串，它将被转换为未正确使用引号括起来的无效`JSON`值（值不会被括在引号中，任何内部引号不会被转义）。

我们建议不要使用该函数。无法在不影响现有用法的情况下修正该函数，可能会在将来的版本中删除该函数。

将指定索引处的元素返回到`json_array`中。索引从零开始：

    SELECT json_array_get('["a", [3, 9], "c"]', 0); -- JSON 'a' (invalid JSON)
    SELECT json_array_get('["a", [3, 9], "c"]', 1); -- JSON '[3,9]'

该函数还支持负索引，以便获取从数组的末尾开始索引的元素：

    SELECT json_array_get('["c", [3, 9], "a"]', -1); -- JSON 'a' (invalid JSON)
    SELECT json_array_get('["c", [3, 9], "a"]', -2); -- JSON '[3,9]'

如果指定索引处的元素不存在，该函数将返回NULL：

    SELECT json_array_get('[]', 0); -- null
    SELECT json_array_get('["a", "b", "c"]', 10); -- null
    SELECT json_array_get('["c", "b", "a"]', -10); -- null

**json\_array\_length(json)** -> bigint

返回`json`（包含JSON数组的字符串）的数组长度：

    SELECT json_array_length('[1, 2, 3]');

**json\_extract(json, json\_path)** -> json

计算`json`（包含JSON的字符串）上的类[JSONPath](https://goessner.net/articles/JsonPath/)表达式`json_path`并将结果作为JSON字符串返回：

    SELECT json_extract(json, '$.store.book');

**json\_extract\_scalar(json, json\_path)** -> varchar

与`json_extract`类似，但将结果值作为字符串返回（而不是编码为JSON）。`json_path`引用的值必须是标量（布尔值、数字或字符串）：

    SELECT json_extract_scalar('[1, 2, 3]', '$[2]');
    SELECT json_extract_scalar(json, '$.store.book[0].author');

**json\_format(json)** -> varchar

返回从输入JSON值序列化的JSON文本。这是`json_parse`的反函数：

    SELECT json_format(JSON '[1, 2, 3]'); -- '[1,2,3]'
    SELECT json_format(JSON '"a"'); -- '"a"'

**注意**

`json_format`和`CAST(json AS VARCHAR)`具有完全不同的语义。

`json_format`将输入JSON值序列化为遵守7159标准的JSON文本。JSON值可以是JSON对象、JSON数组、JSON字符串、JSON数字、`true`、`false`或`null`：

    SELECT json_format(JSON '{"a": 1, "b": 2}'); -- '{"a":1,"b":2}'
    SELECT json_format(JSON '[1, 2, 3]'); -- '[1,2,3]'
    SELECT json_format(JSON '"abc"'); -- '"abc"'
    SELECT json_format(JSON '42'); -- '42'
    SELECT json_format(JSON 'true'); -- 'true'
    SELECT json_format(JSON 'null'); -- 'null'

`CAST(json AS VARCHAR)`将JSON值转换为对应的SQL VARCHAR值。对于JSON字符串、JSON数字、`true`、`false`或`null`，转换行为与对应的SQL类型相同。JSON对象和JSON数组无法转换为VARCHAR：

    SELECT CAST(JSON '{"a": 1, "b": 2}' AS VARCHAR); -- ERROR!
    SELECT CAST(JSON '[1, 2, 3]' AS VARCHAR); -- ERROR!
    SELECT CAST(JSON '"abc"' AS VARCHAR); -- 'abc'; Note the double quote is gone
    SELECT CAST(JSON '42' AS VARCHAR); -- '42'
    SELECT CAST(JSON 'true' AS VARCHAR); -- 'true'
    SELECT CAST(JSON 'null' AS VARCHAR); -- NULL

**json\_parse(string)** -> json

返回从输入JSON文本反序列化的JSON值。这是`json_format`的反函数：

    SELECT json_parse('[1, 2, 3]'); -- JSON '[1,2,3]'
    SELECT json_parse('"abc"'); -- JSON '"abc"'

**注意**

`json_parse`和`CAST(string AS JSON)`具有完全不同的语义。

`json_parse`期望输入一个符合`7159`标准的JSON文本并返回从该JSON文本反序列化的JSON值。JSON值可以是JSON对象、JSON数组、JSON字符串、JSON数字、`true`、`false`或`null`：

    SELECT json_parse('not_json'); -- ERROR!
    SELECT json_parse('["a": 1, "b": 2]'); -- JSON '["a": 1, "b": 2]'
    SELECT json_parse('[1, 2, 3]'); -- JSON '[1,2,3]'
    SELECT json_parse('"abc"'); -- JSON '"abc"'
    SELECT json_parse('42'); -- JSON '42'
    SELECT json_parse('true'); -- JSON 'true'
    SELECT json_parse('null'); -- JSON 'null'

`CAST(string AS JSON)`接受任何VARCHAR值作为输入并返回其值为输入字符串的JSON字符串：

    SELECT CAST('not_json' AS JSON); -- JSON '"not_json"'
    SELECT CAST('["a": 1, "b": 2]' AS JSON); -- JSON '"[\"a\": 1, \"b\": 2]"'
    SELECT CAST('[1, 2, 3]' AS JSON); -- JSON '"[1, 2, 3]"'
    SELECT CAST('"abc"' AS JSON); -- JSON '"\"abc\""'
    SELECT CAST('42' AS JSON); -- JSON '"42"'
    SELECT CAST('true' AS JSON); -- JSON '"true"'
    SELECT CAST('null' AS JSON); -- JSON '"null"'

**json\_size(json, json\_path)** -> bigint

与`json_extract`类似，但返回值的大小。对于对象或数组，该大小为成员数量，标量值的大小为零：

    SELECT json_size('{"x": {"a": 1, "b": 2}}', '$.x'); -- 2
    SELECT json_size('{"x": [1, 2, 3]}', '$.x'); -- 3
    SELECT json_size('{"x": {"a": 1, "b": 2}}', '$.x.a'); -- 0