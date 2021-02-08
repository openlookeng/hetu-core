Rule based optimizations
========================

openLooKeng supports one rule based optimization, described below.

Join Conversion
---------------

Inner-join is a mature operation with great efficiency in our newest version. To utilize the advantage, the semi-join queries can be converted to inner-join for better performance.

With rule-based join conversion, openLooKeng automatically converts the semi-join query to inner-join with equal purpose.

The join conversion is governed by the `rewrite_filtering_semi_join_to_inner_join` session property, with `optimizer.rewrite-filtering-semi-join-to-inner-join` configuration property providing the default value.

The valid values are:
- `true` - enable join conversion
- `false` (default) - disable join conversion

Ranking with TopNRankingNumberNode
----------------------------------------
Ranking operation is optimized with predicate with `TopNRankingNumberNode`. It boosted the performance greatly on ranking queries.

The `TopNRankingNumberNode` ranking is governed by the `optimize_top_n_ranking_number` session property, with `optimizer.optimize-top-n-ranking-function` configuration property providing the default value. 

The valid values are:
- `true` (default) - enable ranking with `TopNRankingNunmberNode`
- `false` - disable ranking with `TopNRankingNumberNode`