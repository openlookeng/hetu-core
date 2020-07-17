
# decimal 函数和运算符

## decimal 字面量

> 可以使用 `DECIMAL 'xxxxxxx.yyyyyyy'` 语法来定义 DECIMAL 类型的字面量。
> 
> DECIMAL 类型的字面量精度将等于字面量（包括尾随零和前导零）的位数。范围将等于小数部分（包括尾随零）的位数。

| 示例字面量| 数据类型|
|:----------|:----------|
| `DECIMAL '0'`| `DECIMAL(1)`|
| `DECIMAL '12345'`| `DECIMAL(5)`|
| `DECIMAL '0000012345.1234500000'`| `DECIMAL(20, 10)`|

## 二进制算术 decimal 运算符

> 支持标准数学运算符。下表说明了结果的精度和范围计算规则。假设 `x` 的类型为 `DECIMAL(xp, xs)`，`y` 的类型为 `DECIMAL(yp, ys)`。

| 运算| 结果类型精度| 结果类型范围|
|----------|----------|----------|
| `x + y` 和 `x - y`| `min(38, 1 + min(xs, ys) + min(xp - xs, yp - ys) )`| `max(xs, ys)`|
| `x * y`| `min(38, xp + yp)`| `xs + ys`|
| `x / y`| `min(38, xp + ys + max(0, ys-xs) )`| `max(xs, ys)`|
| `x % y`| `min(xp - xs, yp - ys) + max(xs, bs)`| `max(xs, ys)`|

> 如果运算的数学结果无法通过结果数据类型的精度和范围精确地表示，则发生异常情况：`Value is out of range`。
> 
> 当对具有不同范围和精度的 decimal 类型进行运算时，值首先被强制转换为公共超类型。对于接近于最大可表示精度 (38) 的类型，当一个操作数不符合公共超类型时，这可能会导致“值超出范围”错误。例如，decimal(38, 0) 和 decimal(38, 1) 的公共超类型是 decimal(38, 1)，但某些符合 decimal(38, 0) 的值无法表示为 decimal(38, 1)。

## 比较运算符

> 所有标准比较运算符和 `BETWEEN` 运算符都适用于 `DECIMAL` 类型。

## 一元 decimal 运算符

> `-` 运算符执行取负运算。结果的类型与参数的类型相同。