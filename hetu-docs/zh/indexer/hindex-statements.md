
# 使用

索引命令行接口集成于`hetu-cli`中, 在安装目录的`bin`目录下运行。


## 创建
索引创建方法如下:
```roomsql  
CREATE INDEX [ IF NOT EXISTS ] index_name
USING [ BITMAP | BLOOM | BTREE | MINMAX ]
ON tbl_name (col_name)
WITH ( 'level' = ['STRIPE', 'PARTITION'], "autoload" = true, "bloom.fpp" = '0.001', "bloom.mmapEnabled" = false, [, …] )
WHERE predicate;
```

- `WHERE` 用于选择部分分区创建索引
- `WITH` 用于设置索引属性。参见各个索引的文档来查看支持的配置
- `"level"='STRIPE'` 如缺省，默认创建级别是STRIPE
- `"autoload"` 覆盖 config.properties 中的默认值 `hetu.heuristicindex.filter.cache.autoload-default`。
索引创建或更新后，是否自动加载到缓存中。
如果为 false，将根据需要加载索引。这意味着，前几个查询可能不会使用索引，因为它正在加载到缓存中。
将此设置为 true 可能会导致高内存使用率，但会提供最佳结果。

如果表是分区的，可以用一个等于表达式来指定一个创建的分区，或使用IN来指定多个。

```roomsql
CREATE INDEX index_name USING bloom ON hive.schema.table (column1);
CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01") WHERE p=part1;
CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WHERE p in (part1, part2, part3);
```

**注意:** 如果表使用多重分区（例如被colA和colB）两列分区，BTree索引仅支持使用**第一级**分区值。Bloom、Bitmap和Minmax索引则支持在任何一个（colA 或 colB）上创建。

## SHOW

显示所有索引或根据名字显示特定索引的信息。
信息包括索引名、用户、表名、索引列、索引类型、索引状态等。

```roomsql
SHOW INDEX;
SHOW INDEX index_name;
```

## UPDATE

如果源表已被修改，则更新现有索引。你可以用```SHOW INDEX index_name```检查索引的状态。

```roomsql
UPDATE INDEX index_name;
```

## DROP

根据名字删除一条索引：
```roomsql
DROP INDEX index_name
WHERE predicate;
```

- `WHERE` 表达式用于只删除索引的几个分区。但是，如果在创建时是全表创建的索引，则只删除部分的索引将不可用（只能整条删除）。

```roomsql
DROP INDEX index_name where p=part1;
```

删除的索引不会立即被从服务器的缓存中清除，直到下一次刷新缓存。刷新时间与设置的缓存加载延迟有关，通常在几秒钟左右。
如果删除源表，索引将自动删除。

## 资源使用说明

### 磁盘使用
启发式索引使用本地临时文件夹创建和处理索引（Linux 上默认为`/tmp`）。
因此，临时文件夹应该有足够的空间。请在 `etc/jvm.config` 中设置以下属性来指定使用的临时文件夹的路径：

```
-Djava.io.tmpdir=/path/to/another/dir
```
