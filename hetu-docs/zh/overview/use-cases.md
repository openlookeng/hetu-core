+++

weight = 1
title = "用例"
+++

# 用例

本节将解析openLooKeng，以便未来的管理员和最终用户了解对openLooKeng的期望。

## openLooKeng不是什么

由于openLooKeng被许多社区成员称为*数据库*，所以可以从openLooKeng不是什么的定义开始。

不要误解openLooKeng理解SQL并提供标准数据库功能的事实。openLooKeng不是通用关系型数据库。它不能替代MySQL、PostgreSQL或Oracle等数据库。openLooKeng不是为处理联机事务处理（OLTP）而设计。对于许多其他为数据仓库或分析而设计和优化的数据库来说，也是如此。

## openLooKeng是什么

openLooKeng是一种使用分布式查询来高效查询大量数据的工具。如果要处理的是TB或PB级的数据，那么你很可能使用与Hadoop和HDFS交互的工具。openLooKeng的设计初衷是替代Hive、Pig等通过MapReduce作业的流水线查询HDFS，但openLooKeng并不局限于访问HDFS。openLooKeng可以并且已经扩展用于操作不同种类的数据源，包括传统的关系型数据库和其他数据源，如Cassandra。

openLooKeng被设计用来处理数据仓库和分析：数据分析、聚合大量数据并生成报告。这些工作负载通常归类为联机分析处理（OLAP）。