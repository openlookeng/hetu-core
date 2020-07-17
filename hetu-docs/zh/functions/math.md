
# 数学函数和运算符

## 数学运算符

| 运算符| 说明| 
|:----------|:----------| 
| `+`| 加| 
| `-`| 减| 
| `*`| 乘| 
| `/`| 除（整数除法会进行截断）| 
| `%`| 取模（余数）| 

## 数学函数

**abs(x)** -> \[与输入相同]

返回`x`的绝对值。

**cbrt(x)** -> double

返回`x`的立方根。

**ceil(x)** -> \[与输入相同]

这是`ceiling`的别名。

**ceiling(x)** -> \[与输入相同]

返回`x`向上舍入到最接近的整数后的值。

**cosine\_similarity(x, y)** -> double

返回稀疏向量`x`和`y`之间的余弦相似度：

    SELECT cosine_similarity(MAP(ARRAY['a'], ARRAY[1.0]), MAP(ARRAY['a'], ARRAY[2.0])); -- 1.0

**degrees(x)** -> double

将角`x`的弧度转换为角度。

**e()** -> double

返回常量欧拉数。

**exp(x)** -> double

返回欧拉数的`x`次幂。

**floor(x)** -> \[与输入相同]

返回`x`向下舍入到最接近的整数后的值。

**from\_base(string, radix)** -> bigint

返回`string`解释为以`radix`为基数的数字后的值。

**inverse\_normal\_cdf(mean, sd, p)** -> double

使用累积概率(p)的给定平均值和标准差(sd)计算正态分布CDF的反函数：P(N \< n)。平均值必须是实数值，标准差必须是正实数值。概率p必须处于区间(0,1)内。

**normal\_cdf(mean, sd, v)** -> double

使用给定的平均值和标准差(sd)计算正态分布CDF：P(N \< v; mean, sd)。平均值和值v必须是实数值，标准差必须是正实数值。

**inverse\_beta\_cdf(a, b, p)** -> double

使用累积概率(p)的给定参数a和b计算Beta CDF的反函数：P(N \< n)。a和b参数必须为正实数值。概率p必须处于区间\[0,1]内。

**beta\_cdf(a, b, v)** -> double

使用给定的参数a和b计算Beta CDF：P(N \< v; a, b)。参数a和b必须为正实数，值v必须为实数。值v必须处于区间\[0,1]内。

**ln(x)** -> double

返回`x`的自然对数。

**log(b, x)** -> double

返回以`b`为底`x`的对数。

**log2(x)** -> double

返回以2为底`x`的对数。

**log10(x)** -> double

返回以10为底`x`的对数。

**mod(n, m)** -> \[与输入相同]

返回`n`除以`m`的模（余数）。

**pi()** -> double

返回常量Pi。

**pow(x, p)** -> double

这是`power`的别名。

**power(x, p)** -> double

返回`x`的`p`次幂。

**radians(x)** -> double

将角`x`的角度转换为弧度。

**rand()** -> double

这是`random()`的别名。

**random()** -> double

返回一个处于范围0.0 \<= x \< 1.0内的伪随机值。

**random(n)** -> \[与输入相同]

返回一个介于0和n之间（不含0和n）的伪随机数。

**round(x)** -> \[与输入相同]

返回`x`舍入到最接近的整数后的值。

**round(x, d)** -> \[与输入相同]

返回`x`舍入到小数点后`d`位后的值。

**sign(x)** -> \[与输入相同]

返回`x`的符号函数，即：

- 0（参数为0）
- 1（参数大于0）
- -1（参数小于0）

对于double参数，该函数还返回：

- NaN（参数为NaN）
- 1（参数为正无穷大）
- -1（参数为负无穷大）

**sqrt(x)** -> double

返回`x`的平方根。

**to\_base(x, radix)** -> varchar

返回以`radix`为基数的`x`表示形式。

**truncate(x)** -> double

返回通过删除小数点后的数字将`x`舍入为整数后的值。

**width\_bucket(x, bound1, bound2, n)** -> bigint

返回具有指定边界`bound1`和`bound2`以及`n`个桶的等宽直方图中`x`的直条数量。

**width\_bucket(x, bins)** -> bigint

根据数组`bins`指定的直条返回`x`的直条数量。`bins`参数必须是double类型的数组，并且假定按升序排序。

## 统计函数

**wilson\_interval\_lower(successes, trials, z)** -> double

返回z分数`z`指定的置信度下伯努利试验过程威尔逊置信区间的下限。

**wilson\_interval\_upper(successes, trials, z)** -> double

返回z分数`z`指定的置信度下伯努利试验过程威尔逊置信区间的上限。

## 三角函数

所有三角函数参数都以弧度表示。请参见单位转换函数`degrees`和`radians`。

**acos(x)** -> double

返回`x`的反余弦值。

**asin(x)** -> double

返回`x`的反正弦值。

**atan(x)** -> double

返回`x`的反正切值。

**atan2(y, x)** -> double

返回`y / x`的反正切值。

**cos(x)** -> double

返回`x`的余弦值。

**cosh(x)** -> double

返回`x`的双曲余弦值。

**sin(x)** -> double

返回`x`的正弦值。

**tan(x)** -> double

返回`x`的正切值。

**tanh(x)** -> double

返回`x`的双曲正切值。

## 浮点函数

**infinity()** -> double

返回表示正无穷大的常数。

**is\_finite(x)** -> boolean

确定`x`是否为有限值。

**is\_infinite(x)** -> boolean

确定`x`是否为无穷大。

**is\_nan(x)** -> boolean

确定`x`是否为非数字值。

**nan()** -> double

返回表示非数字值的常量。