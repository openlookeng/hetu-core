SHOW EXTERNAL FUNCTION
======================

语法
----

``` sql
SHOW EXTERNAL FUNCTION function_name [ ( parameter_type[, ...] ) ]
```

描述
---
输出注册在`function manager`中的`external function`函数信息。
如果指定输入参数，则输出精准匹配输入类型的 `function_name` 的函数信息.

例子
---

例如我们需要显示函数签名为 `example.default.format(double, integer)`的`external function`函数信息:

``` sql
show external function example.default.format(double, integer);
```
``` sql
                                                             External Function                                                              | Argument Types
--------------------------------------------------------------------------------------------------------------------------------------------+-----------------
 External FUNCTION example.default.format (                                                                                                 | double, integer
    k double,                                                                                                                               |
    h integer                                                                                                                               |
 )                                                                                                                                          |
 RETURNS varchar                                                                                                                            |
                                                                                                                                            |
 COMMENT 'format the number ''num'' to a format like''#,###,###.##'', rounded to ''lo'' decimal places, and returns the result as a string' |
 DETERMINISTIC                                                                                                                              |
 RETURNS NULL ON NULL INPUT                                                                                                                 |
 EXTERNAL  
```

显示所有函数名称为 `example.default.format` 的`external function`函数信息:

``` sql
show external function example.default.format;
```
``` sql
                                                             External Function                                                              | Argument Types
--------------------------------------------------------------------------------------------------------------------------------------------+-----------------
External FUNCTION example.default.format (                                                                                                 | double, integer
  k double,                                                                                                                               |
  h integer                                                                                                                               |
)                                                                                                                                          |
RETURNS varchar                                                                                                                            |
                                                                                                                                          |
COMMENT 'format the number ''num'' to a format like''#,###,###.##'', rounded to ''lo'' decimal places, and returns the result as a string' |
DETERMINISTIC                                                                                                                              |
RETURNS NULL ON NULL INPUT                                                                                                                 |
EXTERNAL  
External FUNCTION example.default.format (                                                                                                | real, integer
  k real,                                                                                                                              |
  h integer                                                                                                                               |
)                                                                                                                                          |
RETURNS varchar                                                                                                                            |
                                                                                                                                          |
COMMENT 'format the number ''num'' to a format like''#,###,###.##'', rounded to ''lo'' decimal places, and returns the result as a string' |
DETERMINISTIC                                                                                                                              |
RETURNS NULL ON NULL INPUT                                                                                                                 |
EXTERNAL  
```

参考信息
------
[Function Namespace Managers](../admin/function-namespace-managers.md)