
# 按位运算函数

**bit\_count(x, bits)** -> bigint

获取以二进制补码表示的 `x`（被视为 `bits` 位有符号整数）中设置的位的数量。

    SELECT bit_count(9, 64); -- 2
    SELECT bit_count(9, 8); -- 2
    SELECT bit_count(-7, 64); -- 62
    SELECT bit_count(-7, 8); -- 6

**bitwise\_and(x, y)** -> bigint

返回以二进制补码表示的 `x` 和 `y` 的按位与运算结果。

**bitwise\_not(x)** -> bigint

返回以二进制补码表示的 `x` 的按位取反运算结果。

**bitwise\_or(x, y)** -> bigint

返回以二进制补码表示的 `x` 和 `y` 的按位或运算结果。

**bitwise\_xor(x, y)** -> bigint

返回以二进制补码表示的 `x` 和 `y` 的按位异或运算结果。

另请参见 `bitwise_and_agg` 和 `bitwise_or_agg`。