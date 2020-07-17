
# 逻辑运算符

## 逻辑运算符

| 运算符| 说明| 示例|
|:----------|:----------|:----------|
| `AND`| 如果两个值都为真，则为真。| a AND b|
| `OR`| 如果其中一个值为真，则为真。| a OR b|
| `NOT`| 如果值为假，则为真。| NOT a|

## NULL对逻辑运算符的影响

如果表达式的一侧或两侧为`NULL`，则`AND`比较的结果可能是`NULL`。如果`AND`运算符至少一侧为`FALSE`，则表达式的计算结果为`FALSE`：

    SELECT CAST(null AS boolean) AND true; -- null
    
    SELECT CAST(null AS boolean) AND false; -- false
    
    SELECT CAST(null AS boolean) AND CAST(null AS boolean); -- null

如果表达式的一侧或两侧为`NULL`，则`OR`比较的结果可能是`NULL`。如果`OR`运算符至少一侧为`TRUE`，则表达式的计算结果为`TRUE`：

    SELECT CAST(null AS boolean) OR CAST(null AS boolean); -- null
    
    SELECT CAST(null AS boolean) OR false; -- null
    
    SELECT CAST(null AS boolean) OR true; -- true

下面的真值表展示了如何处理`AND`和`OR`中的`NULL`：

| a| b| a AND b| a OR b|
|:----------|:----------|:----------|:----------|
| `TRUE`| `TRUE`| `TRUE`| `TRUE`|
| `TRUE`| `FALSE`| `FALSE`| `TRUE`|
| `TRUE`| `NULL`| `NULL`| `TRUE`|
| `FALSE`| `TRUE`| `FALSE`| `TRUE`|
| `FALSE`| `FALSE`| `FALSE`| `FALSE`|
| `FALSE`| `NULL`| `FALSE`| `NULL`|
| `NULL`| `TRUE`| `NULL`| `TRUE`|
| `NULL`| `FALSE`| `FALSE`| `NULL`|
| `NULL`| `NULL`| `NULL`| `NULL`|

`NULL`的逻辑补运算结果为`NULL`，如以下示例所示：

    SELECT NOT CAST(null AS boolean); -- null

下面的真值表展示了如何处理`NOT`中的`NULL`：

| a| NOT a|
|:----------|:----------|
| `TRUE`| `FALSE`|
| `FALSE`| `TRUE`|
| `NULL`| `NULL`|

