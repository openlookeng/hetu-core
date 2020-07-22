SELECT
  sum(l.extendedprice)/7.0 as avg_yearly
FROM
  "${database}"."${schema}"."${prefix}lineitem" l,
  "${database}"."${schema}"."${prefix}part" p,
(
SELECT
  0.2*avg(l.quantity) as s_avg, l.partkey as s_partkey
FROM
  "${database}"."${schema}"."${prefix}lineitem" l
GROUP BY
  l.partkey
)
WHERE
  p.partkey = l.partkey
  AND p.brand = 'Brand#43'
  AND p.container = 'LG PACK'
  AND p.partkey = s_partkey
  AND l.quantity < s_avg
;
