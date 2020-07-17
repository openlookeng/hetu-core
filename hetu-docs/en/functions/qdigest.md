
Quantile Digest Functions
=========================

openLooKeng implements the `approx_percentile` function with the quantile digest data structure. The underlying data structure, [qdigest](../language/types.md), is exposed as a data type in openLooKeng, and can be created, queried and stored separately from `approx_percentile`.

Data Structures
---------------

A quantile digest is a data sketch which stores approximate percentile information. The openLooKeng type for this data structure is called `qdigest`, and it takes a parameter which must be one of `bigint`, `double` or `real` which represent the set of numbers that may be ingested by the `qdigest`. They may be merged without losing precision, and for storage and retrieval they may be cast to/from `VARBINARY`.

Functions
---------

**merge(qdigest)** -\> qdigest

Merges all input `qdigest`s into a single `qdigest`.


**value\_at\_quantile(qdigest(T), quantile)** -\> T

Returns the approximate percentile values from the quantile digest given the number `quantile` between 0 and 1.

**values\_at\_quantiles(qdigest(T), quantiles)** -\> T

Returns the approximate percentile values as an array given the input quantile digest and array of values between 0 and 1 which represent the quantiles to return.

**qdigest\_agg(x)** -\> qdigest\<\[same as x\]\>

Returns the `qdigest` which is composed of all input values of `x`.



**qdigest\_agg(x, w)** -\> qdigest\<\[same as x\]\>

Returns the `qdigest` which is composed of all input values of `x` using the per-item weight `w`.

**qdigest\_agg(x, w, accuracy)** -\> qdigest\<\[same as x\]\>

Returns the `qdigest` which is composed of all input values of `x` using the per-item weight `w` and maximum error of `accuracy`. `accuracy` must be a value greater than zero and less than one, and it must be constant for all input rows.
