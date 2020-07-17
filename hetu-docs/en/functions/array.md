
Array Functions and Operators
=============================

Subscript Operator: \[\]
------------------------

The `[]` operator is used to access an element of an array and is indexed starting from one:

    SELECT my_array[1] AS first_element

Concatenation Operator: \|\|
----------------------------

The `||` operator is used to concatenate an array with an array or an element of the same type:

    SELECT ARRAY [1] || ARRAY [2]; -- [1, 2]
    SELECT ARRAY [1] || 2; -- [1, 2]
    SELECT 2 || ARRAY [1]; -- [2, 1]

Array Functions
---------------

**array\_distinct(x)** -\> array

Remove duplicate values from the array `x`.


**array\_intersect(x, y)**-\> array

Returns an array of the elements in the intersection of `x` and `y`, without duplicates.


**array\_union(x, y)** -\> array

Returns an array of the elements in the union of `x` and `y`, without
duplicates.


**array\_except(x, y)** -\> array

Returns an array of elements in `x` but not in `y`, without duplicates.


**array\_join(x, delimiter, null\_replacement)** -\> varchar

Concatenates the elements of the given array using the delimiter and an optional string to replace nulls.


**array\_max(x)** -\> x

Returns the maximum value of input array.


**array\_min(x)** -\> x

Returns the minimum value of input array.


**array\_position(x, element)** -\> bigint

Returns the position of the first occurrence of the `element` in array
`x` (or 0 if not found).


**array\_remove(x, element)** -\> array

Remove all elements that equal `element` from array `x`.


**array\_sort(x)** -\> array

Sorts and returns the array `x`. The elements of `x` must be orderable. Null elements will be placed at the end of the returned array.

**array\_sort(array(T), function(T,T,int)) ** -\> array(T)

Sorts and returns the `array` based on the given comparator `function`. The comparator will take two nullable arguments representing two nullable elements of the `array`. It returns -1, 0, or 1 as the first
nullable element is less than, equal to, or greater than the second nullable element. If the comparator function returns other values (including `NULL`), the query will fail and raise an error :

    SELECT array_sort(ARRAY [3, 2, 5, 1, 2], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))); -- [5, 3, 2, 2, 1]
    SELECT array_sort(ARRAY ['bc', 'ab', 'dc'], (x, y) -> IF(x < y, 1, IF(x = y, 0, -1))); -- ['dc', 'bc', 'ab']
    SELECT array_sort(ARRAY [3, 2, null, 5, null, 1, 2], -- sort null first with descending order
                      (x, y) -> CASE WHEN x IS NULL THEN -1
                                     WHEN y IS NULL THEN 1
                                     WHEN x < y THEN 1
                                     WHEN x = y THEN 0
                                     ELSE -1 END); -- [null, null, 5, 3, 2, 2, 1]
    SELECT array_sort(ARRAY [3, 2, null, 5, null, 1, 2], -- sort null last with descending order
                      (x, y) -> CASE WHEN x IS NULL THEN 1
                                     WHEN y IS NULL THEN -1
                                     WHEN x < y THEN 1
                                     WHEN x = y THEN 0
                                     ELSE -1 END); -- [5, 3, 2, 2, 1, null, null]
    SELECT array_sort(ARRAY ['a', 'abcd', 'abc'], -- sort by string length
                      (x, y) -> IF(length(x) < length(y),
                                   -1,
                                   IF(length(x) = length(y), 0, 1))); -- ['a', 'abc', 'abcd']
    SELECT array_sort(ARRAY [ARRAY[2, 3, 1], ARRAY[4, 2, 1, 4], ARRAY[1, 2]], -- sort by array length
                      (x, y) -> IF(cardinality(x) < cardinality(y),
                                   -1,
                                   IF(cardinality(x) = cardinality(y), 0, 1))); -- [[1, 2], [2, 3, 1], [4, 2, 1, 4]]


**arrays\_overlap(x, y)** -\> boolean

Tests if arrays `x` and `y` have any any non-null elements in common. Returns null if there are no non-null elements in common but either array contains null.


**cardinality(x)** -\> bigint

Returns the cardinality (size) of the array `x`.

**concat(array1, array2, \..., arrayN)**  -\> array

Concatenates the arrays `array1`, `array2`, `...`, `arrayN`. This function provides the same functionality as the SQL-standard concatenation operator (`||`).


**combinations(array(T), n) -\> array(array(T))**

Returns n-element subgroups of input array. If the input array has no duplicates, `combinations` returns n-element subsets:

    SELECT combinations(ARRAY['foo', 'bar', 'baz'], 2); -- [['foo', 'bar'], ['foo', 'baz'], ['bar', 'baz']]
    SELECT combinations(ARRAY[1, 2, 3], 2); -- [[1, 2], [1, 3], [2, 3]]
    SELECT combinations(ARRAY[1, 2, 2], 2); -- [[1, 2], [1, 2], [2, 2]]

Order of subgroups is deterministic but unspecified. Order of elements within a subgroup deterministic but unspecified. `n` must be not be greater than 5, and the total size of subgroups generated must be smaller than 100000.


**contains(x, element) -\> boolean**

Returns true if the array `x` contains the `element`.


**element\_at(array(E), index)** -\> E

Returns element of `array` at given `index`. If `index` \> 0, this function provides the same functionality as the SQL-standard subscript operator (`[]`). If `index` \< 0, `element_at` accesses elements from the last to the first.

**filter(array(T), function(T,boolean))** -\> array(T)

Constructs an array from those elements of `array` for which `function` returns true:

    SELECT filter(ARRAY [], x -> true); -- []
    SELECT filter(ARRAY [5, -6, NULL, 7], x -> x > 0); -- [5, 7]
    SELECT filter(ARRAY [5, NULL, 7, NULL], x -> x IS NOT NULL); -- [5, 7]


**flatten(x)** -\> array

Flattens an `array(array(T))` to an `array(T)` by concatenating the contained arrays.

**ngrams(array(T), n)** -\> array(array(T))

Returns `n`-grams (sub-sequences of adjacent `n` elements) for the `array`. The order of the `n`-grams in the result is unspecified:

    SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 2); -- [['foo', 'bar'], ['bar', 'baz'], ['baz', 'foo']]
    SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 3); -- [['foo', 'bar', 'baz'], ['bar', 'baz', 'foo']]
    SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 4); -- [['foo', 'bar', 'baz', 'foo']]
    SELECT ngrams(ARRAY['foo', 'bar', 'baz', 'foo'], 5); -- [['foo', 'bar', 'baz', 'foo']]
    SELECT ngrams(ARRAY[1, 2, 3, 4], 2); -- [[1, 2], [2, 3], [3, 4]]

**reduce(array(T), initialState S, inputFunction(S,T,S),
outputFunction(S,R))** -\> R

Returns a single value reduced from `array`. `inputFunction` will be invoked for each element in `array` in order. In addition to taking the element, `inputFunction` takes the current state, initially `initialState`, and returns the new state. `outputFunction` will be invoked to turn the final state into the result value. It may be the identity function (`i -> i`). :

    SELECT reduce(ARRAY [], 0, (s, x) -> s + x, s -> s); -- 0
    SELECT reduce(ARRAY [5, 20, 50], 0, (s, x) -> s + x, s -> s); -- 75
    SELECT reduce(ARRAY [5, 20, NULL, 50], 0, (s, x) -> s + x, s -> s); -- NULL
    SELECT reduce(ARRAY [5, 20, NULL, 50], 0, (s, x) -> s + COALESCE(x, 0), s -> s); -- 75
    SELECT reduce(ARRAY [5, 20, NULL, 50], 0, (s, x) -> IF(x IS NULL, s, s + x), s -> s); -- 75
    SELECT reduce(ARRAY [2147483647, 1], CAST (0 AS BIGINT), (s, x) -> s + x, s -> s); -- 2147483648
    SELECT reduce(ARRAY [5, 6, 10, 20], -- calculates arithmetic average: 10.25
                  CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)),
                  (s, x) -> CAST(ROW(x + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
                  s -> IF(s.count = 0, NULL, s.sum / s.count));


**repeat(element, count)** -\> array

Repeat `element` for `count` times.


**reverse(x)** -\> array

Returns an array which has the reversed order of array `x`.


**sequence(start, stop)** -\> array(bigint)

Generate a sequence of integers from `start` to `stop`, incrementing by `1` if `start` is less than or equal to `stop`, otherwise `-1`.

**sequence(start, stop, step)** -\> array(bigint)

Generate a sequence of integers from `start` to `stop`, incrementing by `step`.

**sequence(start, stop)** -\> array(date)

Generate a sequence of dates from `start` date to `stop` date, incrementing by `1` day if `start` date is less than or equal to `stop` date, otherwise `-1` day.

**sequence(start, stop, step)** -\> array(date)

Generate a sequence of dates from `start` to `stop`, incrementing by `step`. The type of `step` can be either `INTERVAL DAY TO SECOND` or `INTERVAL YEAR TO MONTH`.

**sequence(start, stop, step)** -\> array(timestamp)

Generate a sequence of timestamps from `start` to `stop`, incrementing by `step`. The type of `step` can be either `INTERVAL DAY TO SECOND` or `INTERVAL YEAR TO MONTH`.


**shuffle(x)** -\> array

Generate a random permutation of the given array `x`.

**slice(x, start, length)** -\> array

Subsets array `x` starting from index `start` (or starting from the end if `start` is negative) with a length of `length`.

**transform(array(T), function(T,U))** -\> array(U)

Returns an array that is the result of applying `function` to each element of `array`:

    SELECT transform(ARRAY [], x -> x + 1); -- []
    SELECT transform(ARRAY [5, 6], x -> x + 1); -- [6, 7]
    SELECT transform(ARRAY [5, NULL, 6], x -> COALESCE(x, 0) + 1); -- [6, 1, 7]
    SELECT transform(ARRAY ['x', 'abc', 'z'], x -> x || '0'); -- ['x0', 'abc0', 'z0']
    SELECT transform(ARRAY [ARRAY [1, NULL, 2], ARRAY[3, NULL]], a -> filter(a, x -> x IS NOT NULL)); -- [[1, 2], [3]]

**zip(array1, array2\[, \...\])** -\> array(row)

Merges the given arrays, element-wise, into a single array of rows. The M-th element of the N-th argument will be the N-th field of the M-th output element. If the arguments have an uneven length, missing values
are filled with `NULL`. :

    SELECT zip(ARRAY[1, 2], ARRAY['1b', null, '3b']); -- [ROW(1, '1b'), ROW(2, null), ROW(null, '3b')]


**zip\_with(array(T)**, array(U), function(T,U,R)) -\> array(R)

Merges the two given arrays, element-wise, into a single array using `function`. If one array is shorter, nulls are appended at the end to match the length of the longer array, before applying `function`:

    SELECT zip_with(ARRAY[1, 3, 5], ARRAY['a', 'b', 'c'], (x, y) -> (y, x)); -- [ROW('a', 1), ROW('b', 3), ROW('c', 5)]
    SELECT zip_with(ARRAY[1, 2], ARRAY[3, 4], (x, y) -> x + y); -- [4, 6]
    SELECT zip_with(ARRAY['a', 'b', 'c'], ARRAY['d', 'e', 'f'], (x, y) -> concat(x, y)); -- ['ad', 'be', 'cf']
    SELECT zip_with(ARRAY['a'], ARRAY['d', null, 'f'], (x, y) -> coalesce(x, y)); -- ['a', null, 'f']

