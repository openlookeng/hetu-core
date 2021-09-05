
# BloomIndex

BloomIndex使用Bloom Filters(布隆过滤器)来在计划期间和读取数据时进行过滤。

BloomIndex对于具有高基数（有许多不同值）的列以及索引大小很小的列都适用。

布隆过滤器是使用列值构造的。然后在查找过程中，布隆过滤器会告诉我们布隆过滤器中是否有给定值。

BloomIndex仅支持相等表达式，例如`name='monkey'`。

## 使用场景

**注意：当前，启发式索引仅支持ORC存储格式的Hive数据源。**

1. BloomIndex用于调度时的分片过滤，被coordinator节点使用。
2. BloomIndex也用于worker节点上，用于在读取ORC文件是过滤stripes。

## 选择适用的列

在具有高基数（即许多独特值）条件的列上具有过滤predicate的queries可以从BloomIndex中得到好的效果。

例如，类似`SELECT name FROM users WHERE phone=123456789`之类的query
可以通过在`phone`列上使用BloomIndex而得到好的效果，因为列中的数据已被过滤，`phone`列的基数较高。

## 支持的运算符

    =       Equality

## 支持的列类型
    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date", "decimal"

**注意:** 不支持采用其它数据类型来创建index。

## 配置参数

### `bloom.fpp`
 
> -   **类型:** `Double`
> -   **默认值:** `0.001`
> 
> 改变布隆过滤器的FPP (false positive probability)。
> 更小的FPP会提高索引的过滤能力，但是会增加索引的体积。在大多数情况下默认值就足以够用。
> 如果创建的索引太大，可以考虑增加这个值（例如，至0.05)。

### `bloom.mmapEnabled`

> -   **类型:** `Boolean`
> -   **默认值:** `true`
> 
> 控制在读取布隆索引时是否应使用内存映射文件 (mmap)。
> 启用此值将在读取期间将 Bloom 索引缓存到本地磁盘而不是内存中。
> 这将减少内存消耗，但会导致性能略有下降。

## 用例

**创建索引:**
```sql
create index idx using bloom on hive.hindex.users (id);
create index idx using bloom on hive.hindex.users (id) where regionkey=1;
create index idx using bloom on hive.hindex.users (id) where regionkey in (3, 1);
create index idx using bloom on hive.hindex.users (id) WITH ("bloom.fpp" = '0.001');
create index idx using bloom on hive.hindex.users (id) WITH ("bloom.mmapEnabled" = false);
```

* 假设表已按照`regionkey`列分区

**使用:**
```sql
select name from hive.hindex.users where id=123
```

## 如何创建BloomIndex

1. BloomIndex是为每一个Stripe创建的，并让我们知道Stripe是否不包含给定值。 
2. 数据作为列表插入，顺序不重要，并且可以接受重复项。
   对于以下示例，将插入`/hive/database.db/animals/000.orc stripe 1`的数据，如下所示：
   `["Ant", "Crab", "Bat", "Whale", "Ant", "Monkey"]`  
   诸如上次修改时间之类的其他信息将作为元数据存储，以确保不使用陈旧索引。
3. 数据插入完成后，可以将BloomIndex序列化到索引存储中。

![bloom_animal_table](../images/bloom_animal_table.png)

## 如何将BloomIndex用于分片过滤

当OLK引擎需要读取数据时，它会计划Splits（分片）。
每个分片负责读取一部分数据。
例如，当读取具有ORC数据格式的Hive表时，
每个分割将负责读取指定偏移量之间的ORC文件的一部分。

例如，`/hive/database.db/animals/000.orc`，从偏移量`0`开始，从偏移量`2000`开始。

为简单起见，我们可以假定每个分片对应于一个Stripe。

对于类似`SELECT * FROM animals WHERE name='Monkey';`的点查询（point query）
通常将需要读取所有数据，并且过滤将仅应用于与predicates匹配的返回行。
在该示例中，将读取所有四个条带，尽管其中只有一个包含该值。

通过使用BloomIndex，只能调度与predicates匹配的Stripes，因此减少了读取的数据。 这样可以大大减少查询的执行时间。

在此示例中，对`Monkey`的BloomIndex执行查找操作，该操作仅对第一个Stripe返回true。

此外，上次修改时间存储为元数据的一部分，可用于确保索引仍然有效。 如果自创建索引以来已对原始ORC文件进行了修改，则该索引无效，因此不应将其用于过滤。

## 磁盘使用

下面的公式给出了一个对于Bloom索引占用磁盘空间的大致估计。Bloom索引使用的空间大致与用于创建索引的表的大小成正比，同时与指定的`fpp`值的对数相反数成正比。因此，更小的fpp值和更大的数据集会使得创建的索引更大：

索引大小 = -log(fpp) * 表占用空间 * C

系数C还与其他许多因素相关，例如创建索引的列占表总数据的比重，但这些因素的影响应当不如fpp和表的大小重要，且变化较小。作为一个典型的拥有几个列的数据表，这个系数C在0.04左右。这就是说，为一个100GB的数据表的一列创建一个`fpp=0.001`的索引大致需要12GB磁盘空间，而创建`fpp=0.0001`的索引则需要16GB左右。

参见 [hindex-statements](./hindex-statements.md)中的“磁盘使用”章节来指定使用的临时路径。