
# map函数和运算符

## 下标运算符： \[]

`[]`运算符用于从map中检索给定键对应的值：

    SELECT name_to_age_map['Bob'] AS bob_age;

## map函数

**cardinality(x)** -> bigint

返回map `x`的基数（大小）。

**element\_at(map(K,V), key)** -> V

返回给定的`key`的值，如果键不包含在map中，则返回`NULL`。

**map()** -> map\<unknown, unknown>

返回一个空map：

    SELECT map(); -- {}

**map(array(K), array(V))** -> map(K,V)

返回使用给定的键/值数组创建的map：

    SELECT map(ARRAY[1,3], ARRAY[2,4]); -- {1 -> 2, 3 -> 4}

另请参见`map_agg`和`multimap_agg`，以了解如何创建作为聚合的map。

**map\_from\_entries(array(row(K,V)))** -> map(K,V)

返回从给定的项数组创建的map：

    SELECT map_from_entries(ARRAY[(1, 'x'), (2, 'y')]); -- {1 -> 'x', 2 -> 'y'}

**multimap\_from\_entries(array(row(K,V)))** -> map(K,array(V))

返回从给定的项数组创建的多重映射。每个键可以关联多个值：

    SELECT multimap_from_entries(ARRAY[(1, 'x'), (2, 'y'), (1, 'z')]); -- {1 -> ['x', 'z'], 2 -> ['y']}

**map\_entries(map(K,V))** -> array(row(K,V))

返回一个包含给定的map中所有项的数组：

    SELECT map_entries(MAP(ARRAY[1, 2], ARRAY['x', 'y'])); -- [ROW(1, 'x'), ROW(2, 'y')]

**map\_concat(map1(K,V), map2(K,V), ..., mapN(K,V))** -> map(K,V)

返回所有给定的map的并集。如果在多个给定的map中找到某个键，则在生成的map中该键的值来自这些map中的最后一个map。

**map\_filter(map(K,V), function(K,V,boolean))** -> map(K,V)

通过`function`针对其返回true的`map`的项构造一个map：

    SELECT map_filter(MAP(ARRAY[], ARRAY[]), (k, v) -> true); -- {}
    SELECT map_filter(MAP(ARRAY[10, 20, 30], ARRAY['a', NULL, 'c']), (k, v) -> v IS NOT NULL); -- {10 -> a, 30 -> c}
    SELECT map_filter(MAP(ARRAY['k1', 'k2', 'k3'], ARRAY[20, 3, 15]), (k, v) -> v > 10); -- {k1 -> 20, k3 -> 15}

**map\_keys(x(K,V))** -> array(K)

返回map `x`中的所有键。

**map\_values(x(K,V))** -> array(V)

返回map `x`中的所有值。

**map\_zip\_with(map(K,V1), map(K,V2), function(K,V1,V2,V3))** -> map(K,V3)

通过向具有相同键的一对值应用`function`，将两个给定的map合并为单个map。对于仅出现在一个map中的键，会传入NULL以用作缺失的键的值：

    SELECT map_zip_with(MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']), -- {1 -> ad, 2 -> be, 3 -> cf}
                        MAP(ARRAY[1, 2, 3], ARRAY['d', 'e', 'f']),
                        (k, v1, v2) -> concat(v1, v2));
    SELECT map_zip_with(MAP(ARRAY['k1', 'k2'], ARRAY[1, 2]), -- {k1 -> ROW(1, null), k2 -> ROW(2, 4), k3 -> ROW(null, 9)}
                        MAP(ARRAY['k2', 'k3'], ARRAY[4, 9]),
                        (k, v1, v2) -> (v1, v2));
    SELECT map_zip_with(MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 8, 27]), -- {a -> a1, b -> b4, c -> c9}
                        MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]),
                        (k, v1, v2) -> k || CAST(v1/v2 AS VARCHAR));

**transform\_keys(map(K1,V), function(K1,V,K2))** -> map(K2,V)

返回一个向`map`的每个项应用`function`并转换键的map：

    SELECT transform_keys(MAP(ARRAY[], ARRAY[]), (k, v) -> k + 1); -- {}
    SELECT transform_keys(MAP(ARRAY [1, 2, 3], ARRAY ['a', 'b', 'c']), (k, v) -> k + 1); -- {2 -> a, 3 -> b, 4 -> c}
    SELECT transform_keys(MAP(ARRAY ['a', 'b', 'c'], ARRAY [1, 2, 3]), (k, v) -> v * v); -- {1 -> 1, 4 -> 2, 9 -> 3}
    SELECT transform_keys(MAP(ARRAY ['a', 'b'], ARRAY [1, 2]), (k, v) -> k || CAST(v as VARCHAR)); -- {a1 -> 1, b2 -> 2}
    SELECT transform_keys(MAP(ARRAY [1, 2], ARRAY [1.0, 1.4]), -- {one -> 1.0, two -> 1.4}
                          (k, v) -> MAP(ARRAY[1, 2], ARRAY['one', 'two'])[k]);

**transform\_values(map(K,V1), function(K,V1,V2))** -> map(K,V2)

返回一个向`map`的每个项应用`function`并转换值的map：

    SELECT transform_values(MAP(ARRAY[], ARRAY[]), (k, v) -> v + 1); -- {}
    SELECT transform_values(MAP(ARRAY [1, 2, 3], ARRAY [10, 20, 30]), (k, v) -> v + k); -- {1 -> 11, 2 -> 22, 3 -> 33}
    SELECT transform_values(MAP(ARRAY [1, 2, 3], ARRAY ['a', 'b', 'c']), (k, v) -> k * k); -- {1 -> 1, 2 -> 4, 3 -> 9}
    SELECT transform_values(MAP(ARRAY ['a', 'b'], ARRAY [1, 2]), (k, v) -> k || CAST(v as VARCHAR)); -- {a -> a1, b -> b2}
    SELECT transform_values(MAP(ARRAY [1, 2], ARRAY [1.0, 1.4]), -- {1 -> one_1.0, 2 -> two_1.4}
                            (k, v) -> MAP(ARRAY[1, 2], ARRAY['one', 'two'])[k] || '_' || CAST(v AS VARCHAR));