基于规则的优化
===============

openLooKeng支持一种基于规则的优化，如下所述。

Join转换
-------------------------
`inner-join`相较于`semi-join`更为高效。

使用基于规则的join转换， openLooKeng自动将包含semi-join的查询转换为等同意义的inner-join。

join转换由`rewrite_filtering_semi_join_to_inner_join`会话属性控制， 其中`optimizer.rewrite-filtering-semi-join-to-inner-join`配置属性属性提供默认值。

有效值如下：
- `true` - 启用自动转换
- `false` （默认值） - 不启用自动转换

Ranking - TopNRankingNumberNode
-------------------------
Ranking查询中使用了`TopNRankingNumberNode`述语进行了成本优化。

Ranking优化由`optimize_top_n_ranking_number`会话属性控制启用，其中`optimizer.optimize-top-n-ranking-function`配置属性提供默认值。

有效值如下：
- `true` （默认值）- 启用`TopNRankingNumberNode`述语
- `false` - 不启用`TopNRankingNumberNode`述语
