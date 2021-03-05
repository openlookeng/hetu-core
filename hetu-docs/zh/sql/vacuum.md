
# VACUUM

## 摘要

``` sql
VACUUM TABLE table_name [FULL [UNIFY]] [PARTITION partition_value]? [AND WAIT]?
```

## 说明

大数据系统通常使用 HDFS 进行存储，以实现数据在集群中各个节点之间的持久性、透明分布和均衡。HDFS 是一个不可变的文件系统，其间的数据不可编辑，只能追加。为了使用不可变的文件系统，不同的文件格式采用写入新文件以支持数据突变，随后使用异步后台合并来保持性能并避免产生许多小文件。

例如，在 Hive 连接器中，您可以逐行更新或删除 ORC 事务性表。不过，每当运行更新时，就会在 HDFS 文件系统中生成新的增量和 delete\_delta 文件。使用 `VACUUM` 可以将所有这些小文件合并成一个大文件，并优化并行性和性能。

**VACUUM 的类型：**

**默认**

可以将默认 VACUUM 视为合并表的小数据集的第一级。该操作经常发生，通常比 FULL VACUUM 更快。

*Hive：*

在 Hive 连接器中，默认 VACUUM 对应于“轻量级压缩”。将所有有效的增量目录合并到一个压缩的增量目录中，并类似地将所有有效的 delete\_delta 目录合并到一个 delete\_delta 目录中。基本文件不会更改。一旦所有读取方都完成了对较小的旧增量文件的读取，就删除这些文件。

**FULL**

可以将 FULL VACUUM 视为合并表的所有数据集的下一级。与默认 VACUUM 相比，该操作发生的频率更低，完成时间也更长。

**FULL UNIFY**

UNIFY选项有助于将每个分区的多个桶文件合并为单个桶文件，桶号为0。

*Hive：*

在 Hive 连接器中，FULL VACUUM 对应于“重量级压缩”。将所有基本文件和增量文件合并在一起。作为该操作的一部分，会永久删除已删除或更新的行。会从元存储中的事务表中删除所有中止的事务。一旦所有读取方都完成了对旧增量文件的读取，就删除这些文件。

`FULL` 关键字表示是否启动重量级压缩。如果没有该选项，它将执行轻量级压缩。

可以使用 `PARTITION` 子句来指定清理哪个分区。

可以使用 `AND WAIT` 来指定该 VACUUM 以同步模式运行。如果没有该选项，它将以异步模式运行。

## 示例

示例 1：默认 VACUUM，等待完成：

``` sql
    VACUUM TABLE compact_test_table AND WAIT;
```

示例 2：对分区“partition\_key=p1”执行 FULL VACUUM：

``` sql
    VACUUM TABLE compact_test_table_with_partition FULL PARTITION 'partition_key=p1';
```

示例 3：FULL VACUUM，等待完成：

``` sql
    VACUUM TABLE compact_test_table_with_partition FULL AND WAIT;
```

示例 4：对一个分区内的所有小文件进行Unify操作：

```sql
VACUUM TABLE catalog_sales FULL UNIFY PARTITION 'partition_key';
```

## 另请参见

[UPDATE](./update.md)、[DELETE](./delete.md)