
Cost based optimizations
========================

openLooKeng supports several cost based optimizations, described below.

Join Enumeration
----------------

The order in which joins are executed in a query can have a significant impact on the query\'s performance. The aspect of join ordering that has the largest impact on performance is the size of the data being processed and transferred over the network. If a join that produces a lot of data is performed early in the execution, then subsequent stages will need to process large amounts of data for longer than necessary,
increasing the time and resources needed for the query.

With cost based join enumeration, openLooKeng uses `/optimizer/statistics` provided by connectors to estimate the costs for different join orders and automatically pick the join order with the lowest computed costs.

The join enumeration strategy is governed by the `join_reordering_strategy` session property, with the
`optimizer.join-reordering-strategy` configuration property providing the default value.

The valid values are:

- `AUTOMATIC` (default) - full automatic join enumeration enabled
- `ELIMINATE_CROSS_JOINS` - eliminate unnecessary cross joins
- `NONE` - purely syntactic join order

If using `AUTOMATIC` and statistics are not available, or if for any other reason a cost could not be computed, the `ELIMINATE_CROSS_JOINS` strategy is used instead.

Join Distribution Selection
---------------------------

openLooKeng uses a hash based join algorithm. That implies that for each join operator a hash table must be created from one join input (called build side). The other input (probe side) is then iterated and for each row the hash table is queried to find matching rows.

There are two types of join distributions:

- Partitioned: each node participating in the query builds a hash table from only fraction of the data
- Broadcast: each node participating in the query builds a hash table from all of the data (data is replicated to each node)

Each type have their trade offs. Partitioned joins require redistributing both tables using a hash of the join key. This can be slower (sometimes substantially) than broadcast joins, but allows much larger joins. In particular, broadcast joins will be faster if the build side is much smaller than the probe side. However, broadcast joins require that the tables on the build side of the join after filtering fit in memory on each node, whereas distributed joins only need to fit in distributed memory across all nodes.

With cost based join distribution selection, openLooKeng automatically chooses to use a partitioned or broadcast join. With cost based join enumeration, openLooKeng automatically chooses which side is the probe and
which is the build.

The join distribution strategy is governed by the `join_distribution_type` session property, with the
`join-distribution-type` configuration property providing the default value.

The valid values are:

- `AUTOMATIC` (default) - join distribution type is determined automatically for each join
- `BROADCAST` - broadcast join distribution is used for all joins
- `PARTITIONED` - partitioned join distribution is used for all join

Dynamic Filter Creation
-------------------------
Dynamic filters are created for all `JoinNode` at the planning phase. However, due to the mechanism of dynamic filtering, under different scenarios, dynamic filters are harming the performance. The waiting for the build side of `JoinNode` that has high selectivity on the deep-subtrees' dynamic filtering does not help the performance, while dynamic filtering on low selectivity hurts the performance. To accommodate both cases, openLooKeng can optimize the dynamic filter creation based on this characteristic.

The dynamic filter creation is governed by the `optimize_dynamic_filter_generation` session property.

The valid values are:
- `true` (default) - enable dynamic filter creation
- `false` - disable dynamic filter creation

Non-estimable Predicate approximation
----------------------------------------

Enables approximation of the output row count of filters whose costs cannot be accurately estimated even with complete statistics. This allows the optimizer to
produce more efficient plans in the presence of filters which were previously not estimated.

This is governed by `non_estimatable_predicate_approximation_enabled` session property, with `optimizer.non-estimatable-predicate-approximation.enabled` configuration property providing the default value.

The valid values are:
- `true` (default) - enable non-estimatable predicate approximation
- `false` - disable non-estimatable predicate approximation

Connector Implementations
-------------------------

In order for the openLooKeng optimizer to use the cost based strategies, the connector implementation must provide `statistics`.
