
# 使用

索引命令行接口集成于`hetu-cli`中, 在安装目录的`bin`目录下运行。


## 创建
索引创建方法如下:
```roomsql  
CREATE INDEX [ IF NOT EXISTS ] index_name
USING [ BITMAP | BLOOM | BTREE | MINMAX ]
ON tbl_name (col_name)
WITH ( 'level' = ['STRIPE', 'PARTITION'], "bloom.fpp" = '0.001', [, …] )
WHERE predicate;
```

- `WHERE` 用于选择部分分区创建索引
- `WITH` 用于设置索引属性。参见各个索引的文档来查看支持的配置
- `"level"='STRIPE'` 如缺省，默认创建级别是STRIPE

如果表是分区的，可以用一个等于表达式来指定一个创建的分区，或使用IN来指定多个。

```roomsql
CREATE INDEX index_name USING bloom ON hive.schema.table (column1);
CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01") WHERE p=part1;
CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WHERE p in (part1, part2, part3);
```

**注意:** 如果表使用多重分区（例如被colA和colB）两列分区，则索引创建仅支持使用**第一级**分区值。

## SHOW

显示所有索引或只根据名字显示一个索引：

```roomsql
SHOW INDEX;
SHOW INDEX index_name;
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

## 资源使用说明

### 磁盘使用
启发式索引使用本地临时文件夹存储创建的索引，然后打包上传至hdfs。因此，它需要临时文件夹（例如，linux上的`/tmp`)挂载的磁盘分区在本地有足够的可用空间。如果挂载的磁盘分区可用空间不足，用户可以在worker节点的`jvm.config`中通过`-Djava.io.tmpdir`来指定使用的临时路径：

```
-Djava.io.tmpdir=/path/to/another/dir
```
