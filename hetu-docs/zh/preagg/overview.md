# StarTree多维数据集
## 介绍
StarTree Cube，作为多维数据集，是存储为表格的物化预聚合结果。该技术旨在优化低延迟冰山查询。
冰山查询是涉及**GROUP BY**和**HAVING**子句的SQL查询的一种特殊情况，其中答案集相对于扫描的数据大小而言较小。
查询的特点是输入量大，输出量小。

此技术允许用户在现有表上构建Cubes，其中包含旨在优化特定查询的聚合和维度。
Cubes是汇总预聚合，与原始表相比，其维度和行数更少。
较少的行数意味着花费在表扫描上的时间显着减少，从而减少查询延迟。
如果查询是预聚合表的维度和度量的子集，
那么Cube可以用来计算查询，而无需访问原始表。

Cube有以下几个属性
 - Cubes以表格格式存储
 - 一般来说，可以为任何连接器中的任何表创建Cubes并存储在另一个连接器中
 - 通过重写逻辑计划以使用Cube而不是原始表来减少查询延迟。

## Cube的多维数据集优化器规则
作为逻辑计划优化的一部分，Cube优化器规则使用Cubes分析和优化逻辑计划的聚合子树。
该规则查找通常如下所示的聚合子树

```
AggregationNode
|- ProjectNode[Optional]
  |- ProjectNode[Optional]
    |- FilterNode[Optional]
      |- ProjectNode[Optional]
        |- TableScanNode
```

规则通过子树解析，识别出与Cube元数据匹配的表名、聚合函数、where子句、group by子句
识别任何可以帮助优化查询的Cube。在多个匹配的情况下，选择最近创建的Cube进行优化。如果找到任何匹配项，则整个
使用Cube重写聚合子树。此优化器使用TupleDomain构造来匹配查询中提供的谓词是否可以被
立方体。

下图描绘了优化后逻辑计划的变化。

![img](../images/cube-logical-plan-optimizer.png)

## 推荐用法
1. Cubes对于需要大量输入并产生少量输入的冰山查询最有用。
2. 当Cube的大小小于构建Cube的实际表上的大小时，查询性能最佳。
3. 如果源表更新，则需要重建Cubes。

**注意：**
如果在构建Cubes后更新源表，Cube优化器将忽略在表上创建的Cubes。原因是，对更新的任何操作都被视为对现有数据的更改，即使在原始表中只插入了新行。由于无法区分插入和更新，因此不能使用多维数据集，因为它可能会导致不正确的结果。我们正在研究解决此限制的解决方案。

## 支持的连接器
以下是用于存储Cube的支持的连接器
1. Hive
2. Memory

以下连接器中的表可用作构建StarTree　Cube
1. Hive
2. Memory
3. Clickhouse

## 未来的工作
1. 支持更多JDBC连接器
2. 简化Cube管理

   2.1. 克服为更大的数据集创建Cube的限制。

## 启用和禁用StarTree Cube
启用：
```sql 
SET SESSION enable_star_tree_index=true;
```
禁用：
```sql 
SET SESSION enable_star_tree_index=false;
```

## 配置属性
| 属性名称                                           | 默认值                | 是否必要 | 描述          |
|---------------------------------------------------|---------------------|---------|--------------|
| optimizer.enable-star-tree-index                  | false               | 否      | 启动StarTree Cube |
| cube.metadata-cache-size                          | 50                  | 否      | 在驱逐发生之前可以加载到缓存中的 StarTree Cube 的最大元数据数 |
| cube.metadata-cache-ttl                           | 1h                  | 否      | 在驱逐发生之前加载到缓存中的 StarTree Cube 的最大生存时间 |

## 依赖关系

StarTree Cube依赖于Hetu Metastore来存储Cube相关的元数据。
请查看[Hetu Metastore](../admin/meta-store.md)以获取更多信息。

## 例子

创建StarTree Cube：
```sql 
CREATE CUBE nation_cube 
ON nation 
WITH (AGGREGATIONS=(count(*), count(distinct regionkey), avg(nationkey), max(regionkey)),
GROUP=(nationkey),
format='orc', partitioned_by=ARRAY['nationkey']);
```
接下来，将数据添加到Cube：
```sql 
INSERT INTO CUBE nation_cube WHERE nationkey >= 5;
```
使用WHERE子句创建StarTree Cube：
请注意，以下查询仅通过CLI支持

```sql 
CREATE CUBE nation_cube 
ON nation 
WITH (AGGREGATIONS=(count(*), count(distinct regionkey), avg(nationkey), max(regionkey)),
GROUP=(nationkey),
format='orc', partitioned_by=ARRAY['nationkey'])
WHERE nationkey >= 5;
```

当需要使用新的Cube时，只需使用包含在Cube中的聚合查询原始表：

```sql 
SELECT count(*) FROM nation WHERE nationkey >= 5 GROUP BY nationkey;
SELECT nationkey, avg(nationkey), max(regionkey) FROM nation WHERE nationkey >= 5 GROUP BY nationkey;
```

由于插入Cube的数据是为`nationkey >= 5`，只有匹配此条件的查询才会使用Cube。
不符合条件的查询将继续工作，但不会使用Cube。

如果Cube的源表更新，则对应的Cube自动过期。为了克服这个问题，我们通过引入**RELOAD CUBE**命令在openLooKeng CLI中添加了支持。如果Cube的状态变为“未激活”或“过期”，用户将能够手动重新加载Cube。重新加载Cube`nation_cube`的语法如下:

```sql 
RELOAD CUBE nation_cube
```

请注意，此功能仅在CLI支持。在重新加载过程中，如果发生意外错误，用户可以查看原始SQL语句，手动重新创建Cube。

## 为大型数据集构建Cube
当前实现的限制之一是不能一次为更大的数据集构建Cube。这是由于集群内存限制。
处理大量行需要比集群配置更多的内存。这会导致查询失败并显示消息**Query exceeded per-node user memory limit**，也就是警告查询超出每节点用户内存限制。为了克服这个问题，**INSERT INTO CUBE** SQL支持被添加了。
用户可以通过执行多个操作来为更大的数据构建一个Cube插入到Cube语句中。insert语句接受一个where子句，它可以用来限制处理和插入到Cube中的数量。

本节介绍为更大的数据集构建Cube的步骤。

让我们以TPCDS数据集和`store_sales`表为例。该表有10年的数据，
用户想要构建2001年的Cube，由于集群内存限制，无法一次处理2001年的整个数据集。

```sql
CREATE CUBE store_sales_cube ON store_sales WITH (AGGREGATIONS = (sum(ss_net_paid), sum(ss_sales_price), sum(ss_quantity)), GROUP = (ss_sold_date_sk, ss_store_sk));

SELECT min(d_date_sk) as year_start, max(d_date_sk) as year_end FROM date_dim WHERE d_year = 2001;
 year_start | year_end 
------------+----------
    2451911 |  2452275 
(1 row)
```
如果需要处理的行数很大并且查询内存超过为集群配置的限制，
则以下查询可能会导致失败。

```sql
INSERT INTO CUBE store_sales_cube WHERE ss_sold_date_sk BETWEEN 2451911 AND 242275; 
```

### 解决方案1)
为了克服这个问题，可以使用多个insert语句来处理行并插入cube中，并且可以使用where子句来限制行数；

```sql
INSERT INTO CUBE store_sales_cube WHERE ss_sold_date_sk BETWEEN 2451911 AND 2452010;
INSERT INTO CUBE store_sales_cube WHERE ss_sold_date_sk >= 2452011 AND ss_sold_date_sk <= 2452110;
INSERT INTO CUBE store_sales_cube WHERE ss_sold_date_sk BETWEEN 2452111 AND 2452210;
INSERT INTO CUBE store_sales_cube WHERE ss_sold_date_sk BETWEEN 2452211 AND 2452275;
```

### 解决方案2)
CLI已被修改以支持为更大的数据集创建Cubes，而不需要多个插入语句。CLI在内部处理这个过程。
一旦用户运行带有where子句的create cube语句，CLI就会负责创建Cube并将数据插入其中。
此过程改善了用户体验并改善了基于集群内存限制的内存占用。CLI在内部将转换语句解析为一个create Cube语句，然后是一个或多个insert语句。
此更改仅在用户从CLI而非通过任何其他方式（例如JDBC等）执行命令时才有效。

```sql
CREATE CUBE store_sales_cube ON store_sales WITH (AGGREGATIONS = (sum(ss_net_paid), sum(ss_sales_price), sum(ss_quantity)), GROUP = (ss_sold_date_sk, ss_store_sk)) WHERE ss_sold_date_sk BETWEEN 2451911 AND 242275;
```

系统内部会重写所有连续的范围谓词并将其合并为单个谓词；

```sql
SHOW CUBES;

           Cube Name             |         Table Name         | Status |         Dimensions          |                     Aggregations                      |                                     Where Clause                                     
---------------------------------+----------------------------+--------+-----------------------------+-------------------------------------------------------+-------------------------------------------------------+------------------------------
 hive.tpcds_sf1.store_sales_cube | hive.tpcds_sf1.store_sales | Active | ss_sold_date_sk,ss_store_sk | sum(ss_sales_price),sum(ss_net_paid),sum(ss_quantity) | (("ss_sold_date_sk" >= BIGINT '2451911') AND ("ss_sold_date_sk" < BIGINT '2452276')) 
```

**注意：**

① 系统将尝试将所有类型的Predicates重写为Range以查看它们是否可以合并在一起。
   所有连续谓词将合并为单个范围谓词，其余谓词保持不变。

   仅支持以下类型并且可以合并在一起。
   `Integer, TinyInt, SmallInt, BigInt, Date, String`

   对于字符串数据类型，谓词合并逻辑仅在字符串以数字结尾，并且所有字符串的长度相同时才能生效。例如，

```sql
   INSERT INTO CUBE store_sales_cube WHERE store_id BETWEEN 'A01' AND 'A10';
   INSERT INTO CUBE store_sales_cube WHERE store_id BETWEEN 'A11' AND 'A20';
```
   插入后，两个谓词将被合并至`'A01' AND 'A20'`。
   
   ```sql
      SELECT ss_store_id, sum(ss_sales_price) WHERE ss_store_id BETWEEN 'A05' AND 'A15'; - Cube 能被这个查询语句所使用
   ```
   
   以下示例中，`store_id`值的长度不相同。
   
   ```
      INSERT INTO CUBE store_sales_cube WHERE store_id = 'A1';
      INSERT INTO CUBE store_sales_cube WHERE store_id = 'A2' 
   ```
   
   根据varchar谓词合并逻辑，store_id谓词将被重写为`store_id >= 'A1' and store < 'A3'`；
   
   ```
      INSERT INTO CUBE store_sales_cube WHERE store_id = 'A10' 
   ```
   
   上述查询将失败，因为`A10`是范围`store_id >= 'A1' and store < 'A3'`的子集。请用户注意这个问题。
   
   对于其他数据类型，很难识别两个谓词是否连续，因此它们无法被合并。因此，即使某些Cube具有所有所需的数据，也可能不会被用来优化查询。
   
② 谓词重写也有一些限制。如以下查询：
   
   ```sql   
      INSERT INTO CUBE store_sales_cube WHERE ss_sold_date_sk > 2451911; 
   ```
   
   谓词重写为ss_sold_date_sk >= 2451912为合并连续谓词做准备。
   由于谓词已重写，使用ss_sold_date_sk > 2451911谓词进行查询将无法匹配到Cube谓词，因此不会使用Cube优化查询。同样的情况也适用于具有<=运算符的谓词。例如 ss_sold_date_sk <= 2451911重写为ss_sold_date_sk < 2451912。
   
   ```sql   
      SELECT ss_sold_date_sk, .... FROM hive.tpcds_sf1.store_sales WHERE ss_sold_date_sk > 2451911
   ```
   
③ 只能合并单列谓词。

## 未解决的问题和限制
1. StarTree Cube仅在按基数分组的数量远小于源表中的行数时有效。
2. 维护大型数据集的Cubes需要大量的用户工作。
3. 仅支持增量插入Cube。无法从Cube中删除特定行。
4. 即使源表尚未更新，在事务表上创建的Cubes也可能会自动过期。
   这是由于压缩策略将delta文件合并为单个大型ORC文件，这反过来又更改了表的最后修改时间。
   Cube状态是通过比较创建Cube时表的最后修改时间戳与执行查询时表的最后修改时间来确定的。
5. openLooKeng CLI已经过修改，以简化为更大的数据集创建Cubes的过程。 
   但是这种实现仍然存在局限性，因为该过程涉及将多个Cube谓词合并为一个。
   只有定义在Integer、Long和Date类型上的Cube谓词才能正确合并。 对Char、String类型的支持仍需实现。
6. 当Varchar类型的谓词的数值长度是一样时可合并。

## Star Tree上的性能优化
1. 对同一个group by列的星型查询重写优化：如果查询语句与Cube组匹配，则会改写查询计划将聚合运算结果重定向到Cube结果，否则将添加其他聚合结果内部应用于重写语句。
2. 平均聚合函数的star tree表扫描优化：如果查询语句与group by列的Cube匹配，则会改写查询计划将聚合运算结果重定向到Cube的预聚合列的平均值结果，否则语句将在内部重写，以选择star tree预聚合Sum和Count结果，随后计算平均值。