
HyperLogLog Functions
=====================

openLooKeng implements the `approx_distinct` function using the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) data structure.

Data Structures
---------------

openLooKeng implements HyperLogLog data sketches as a set of 32-bit buckets which store a *maximum hash*. They can be stored sparsely (as a map from bucket ID to bucket), or densely (as a contiguous memory block). The HyperLogLog data structure starts as the sparse representation, switching to dense when it is more efficient. The P4HyperLogLog structure is initialized densely and remains dense for its lifetime.

`hyperloglog_type` implicitly casts to `p4hyperloglog_type`, while one can explicitly cast `HyperLogLog` to `P4HyperLogLog`:

    cast(hll AS P4HyperLogLog)

Serialization
-------------

Data sketches can be serialized to and deserialized from `varbinary`. This allows them to be stored for later use. Combined with the ability  to merge multiple sketches, this allows one to calculate `approx_distinct` of the elements of a partition of a query, then for the entirety of a query with very little
cost.

For example, calculating the `HyperLogLog` for daily unique users will allow weekly or monthly unique users to be calculated incrementally by combining the dailies. This is similar to computing weekly revenue by
summing daily revenue. Uses of `approx_distinct` with `GROUPING SETS` can be converted to use `HyperLogLog`.
**Examples**

    CREATE TABLE visit_summaries (
      visit_date date,
      hll varbinary
    );
    
    INSERT INTO visit_summaries
    SELECT visit_date, cast(approx_set(user_id) AS varbinary)
    FROM user_visits
    GROUP BY visit_date;
    
    SELECT cardinality(merge(cast(hll AS HyperLogLog))) AS weekly_unique_users
    FROM visit_summaries
    WHERE visit_date >= current_date - interval '7' day;

Functions
---------

**approx\_set(x)** -\> HyperLogLog

Returns the `HyperLogLog` sketch of the input data set of `x`. This data sketch underlies `approx_distinct` and can be stored and used later by calling `cardinality()`.


**cardinality(hll)** -\> bigint

This will perform `approx_distinct` on the data summarized by the `hll` HyperLogLog data sketch.


**empty\_approx\_set()** -\> HyperLogLog

Returns an empty `HyperLogLog`.


**merge(HyperLogLog)** -\> HyperLogLog

Returns the `HyperLogLog` of the aggregate union of the individual `hll` HyperLogLog structures.

