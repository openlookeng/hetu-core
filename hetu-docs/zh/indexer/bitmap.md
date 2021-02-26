
# BitmapIndex（位图索引）

BitmapIndex使用位图来进行早期行过滤，这可以帮助减少CPU和内存使用量。
这在高并发queries中是有益的。

BitmapIndex对于低基数（即独特数据不多的）的列效果很好，
因为index的大小随着独特数量的增加而增加。
例如，`gender`之类的列将具有较小的尺寸。
而像`id`这样的列将具有一个极高的大小（不推荐）。

Bitmap是为每个独特列值而构造一个位图，可以用来记录并且在其中找到该值的行号。
然后，B+Tree会被用来存储值与其位图之间的映射。
通过使用B+Tree，BitmapIndex可以支持使用运算符之类的范围query，例如
大于（`>`），小于（`<`），`BETWEEN`等。

**注意：** 在ORC算子下推启用时，BitmapIndex效果更好。可以通过设置`hive.properties`中的`hive.orc-predicate-pushdown-enabled=true`来启用，
或者在命令行中启用`set session hive.orc_predicate_pushdown_enabled=true;`。

参见[Properties](../admin/properties.md)获得更多信息。

## 使用场景

**注意：当前，启发式索引仅支持ORC存储格式的Hive数据源。**

BitmapIndex用于过滤从ORC文件中读取的数据，且仅供worker节点使用。

## 选择适用的列

以高并发率运行的queries，并且在具有低基数（独特值不多的）条件的列上具有过滤predicates
可以从BitmapIndex中得到好的效果。

例如，类似`SELECT * FROM Employees WHERE gender='M' AND type='FULLTIME' AND salary>10000`的query
可以在`gender`和`type`列上用BitmapIndex并且得到好的效果，因为数据在两列上都被过滤，并且两者的基数都很低。

## 支持的运算符

    =       Equality
    >       Greater than
    >=      Greater than or equal
    <       Less than
    <=      Less than or equal
    BETWEEN Between range
    IN      IN set
    
## 支持的列类型
    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date"

## 用例

**创建：**
```sql
create index idx using bitmap on hive.hindex.users (gender);
create index idx using bitmap on hive.hindex.users (gender) where regionkey=1;
create index idx using bitmap on hive.hindex.users (gender) where regionkey in (3, 1);
```

* 假设表已按照`regionkey`列分区

**使用:**
```sql
select * from hive.hindex.users where gender="female"
select * from hive.hindex.users where age>20
select * from hive.hindex.users where age<25
select * from hive.hindex.users where age>=21
select * from hive.hindex.users where age<=24
select * from hive.hindex.users where age between (20, 25)
select * from hive.hindex.users where age in (22, 23)
```

## 如何创建BitmapIndex

1. BitmapIndex是为每一个在ORC文件中的Stripe创建的，并使我们知道哪些行包含值。
2. 数据作为有序列表插入，数据顺序是根据在Stripe中的出现顺序。
   对于以下示例，`/hive/database.db/animals/000.orc stripe 1`的数据将如下插入：  
   `["Ant", "Crab", "Bat", "Whale", "Ant", "Monkey"]`  
   诸如上次修改时间之类的其他信息将作为元数据存储，以确保不使用陈旧索引。
3. 数据插入完成后，将为每个独特值创建一个Bitmap。这是一种跟踪值存在的行的紧凑方式。（请参见表）
4. 一旦为独特值创建了Bitmap。该值和相应的Bitmap被压缩并存储在B+Tree中，以允许在`O(log(n))`之内的运行速度来快速查找。

![bitmap_animal_table](../images/bitmap_animal_table.png)

![bitmap_stripe_table](../images/bitmap_stripe_table.png)

![bitmap_animal_diagram](../images/bitmap_animal_diagram.png)

## 如何将BitmapIndex用于行过滤

对于诸如`SELECT * FROM Animal WHERE type = LAND`之类的过滤器queries，通常，所有数据都需要读入内存，并且过滤将仅应用于与predicates匹配的返回行。

例如，对于`/hive/database.db/animals/000.orc stripe 1`，以下数据将被读入内存：
```
Ant, LAND  
Crab, WATER  
Bat, AERIAL  
Whale, WATER  
Ant, LAND  
Monkey, LAND  
```
然后，将应用过滤以删除与predicate不匹配的行：
```
Ant, LAND  
Ant, LAND  
Monkey, LAND  
```
通过使用BitmapIndex，我们可以改进此过程。而不是读取Stripe中的所有行。
BitmapIndex可以返回应读取的匹配行的列表。这样既可以减少内存消耗，又可以缩短查询执行时间。

如果我们在`type`列上创建BitmapIndex，则在从Stripe读取数据之前，
将为Stripe的BitmapIndex查询`LAND`，并将返回具有以下值的迭代器：
`[1, 5, 6]`

这些对应于与值匹配的行号（即仅应将这些行读入内存），其余的可以跳过。

对于具有多个值的queries，例如`SELECT * FROM animes WHERE type=LAND OR type=AERIAL;`，
BitmapIndex将执行两次查找。将对两个Bitmaps执行联合以得到最终结果
（例如，`[001000] UNION [100011] = [101011]`），因此返回的迭代器将为`[1、3、5、6]`。
