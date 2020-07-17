
Bitwise Functions
=================

**bit\_count(x, bits)** -\> bigint

Count the number of bits set in `x` (treated as `bits`-bit signed integer) in 2\'s complement representation:

    SELECT bit_count(9, 64); -- 2
    SELECT bit_count(9, 8); -- 2
    SELECT bit_count(-7, 64); -- 62
    SELECT bit_count(-7, 8); -- 6

**bitwise\_and(x, y)** -\> bigint

Returns the bitwise AND of `x` and `y` in 2\'s complement representation.

**bitwise\_not(x)** -\> bigint

Returns the bitwise NOT of `x` in 2\'s complement representation.

**bitwise\_or(x, y)** -\> bigint

Returns the bitwise OR of `x` and `y` in 2\'s complement representation.

**bitwise\_xor(x, y)** -\> bigint

Returns the bitwise XOR of `x` and `y` in 2\'s complement representation.


See also `bitwise_and_agg` and `bitwise_or_agg`.
