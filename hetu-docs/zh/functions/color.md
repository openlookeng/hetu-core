
# 颜色函数

**bar(x, width)** -> varchar

使用默认的`low_color` 红色和 `high_color` 绿色呈现 ANSI 条形图中的单个条形。例如，将 `x` 值 25% 和 width 值 40 传递给该函数，则呈现一个 10 字符红色条形图，后跟 30 个空格，从而创建一个 40 字符条形图。

**bar(x, width, low\_color, high\_color)** -> varchar

呈现 ANSI 条形图中具有指定 `width` 的单个行。参数 `x` 是处于 \[0,1] 范围之内的 double 值。处于范围 \[0,1] 之外的 `x` 值将被截断为值 0 或 1。`low_color` 和 `high_color` 捕获颜色以用于水平条形图的两端。例如，如果 `x` 为 0.5，`width` 为 80，`low_color` 为 0xFF0000，`high_color` 为 0x00FF00，则该函数返回一个 40 字符条形，其颜色从红色 (0xFF0000) 变为黄色 (0xFFFF00)，使用空格对 80 字符条形的其余部分进行填充。

![img](../images/functions_color_bar.png)

**color(string)** -> color

从格式为“#000”的 4 字符字符串捕获解码的 RGB 值，返回相应的颜色。输入字符串应该为 varchar，其中包含 CSS 样式的短 RGB 字符串，或者为 `black`、`red`、`green`、`yellow`、`blue`、`magenta`、`cyan` 和 `white` 之一。

**color(x, low, high, low\_color, high\_color)** -> color

返回一个介于 `low_color` 和 `high_color` 之间的颜色，使用 double 参数 `x`、`low` 和 `high` 计算得出一个小数，然后将该小数传给下面显示的 `color(fraction, low_color, high_color)` 函数。如果 `x` 处于 `low` 和 `high` 定义的范围之外，则对其值进行截断，以使其处于该范围之内。

**color(x, low\_color, high\_color)** -> color

根据介于 0 和 1.0 之间的 double 参数 `x` 返回一个介于 `low_color` 和 `high_color` 之间的颜色。参数 `x` 是处于 \[0,1] 范围之内的 double 值。处于范围 \[0,1] 之外的 `x` 值将被截断为值 0 或 1。

**render(x, color)** -> varchar

使用特定的颜色（使用 ANSI 颜色代码）呈现值 `x`。`x` 可以为 double、bigint 或 varchar 类型。

**render(b)** -> varchar

接受 boolean 值 `b` 并使用 ANSI 颜色代码将绿色呈现为 true 或将红色呈现为 false。

**rgb(red, green, blue)** -> color

返回一个颜色值，捕获三个作为 int 参数（范围为 0 至 255）提供的分量颜色值的 RGB 值：`red`、`green`、`blue`。

