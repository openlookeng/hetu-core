# Release 1.4.1 (2021年11月12日)

## 关键特性

本次发布主要新增了OmniData Connector的介紹和ARM架构下支持JDK8。

| 类别                    | 特性                                                      | PR #s                                                        |
| ----------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 数据源               | openLooKeng支持通过OmniData Connector将算子卸载到近数据侧执行，减少无效数据在网络上的传输，有效提升大数据计算性能。                                               | 1219                                                          |
| ARM架构 | 消除因JDK卡顿问题导致ARM架构下对java版本的强制要求，支持ARM架构下使用jdk1.8.262及以上版本。 | 1214                                                           |

## 获取文档

请参考：[https://gitee.com/openlookeng/hetu-core/tree/1.4.1/hetu-docs/zh](https://gitee.com/openlookeng/hetu-core/tree/1.4.1/hetu-docs/zh )