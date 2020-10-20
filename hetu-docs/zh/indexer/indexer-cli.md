
# 索引命令行接口

##  用法

索引命令行接口集成于`hetu-cli`中, 在安装目录的`bin`目录下运行。

命令的使用方式如下:
```roomsql
CREATE INDEX [ IF NOT EXISTS ] index_name
USING [ BITMAP | BLOOM | MINMAX ]
ON tbl_name (col_name)
WITH ( "bloom.fpp" = '0.001', [, …] )
WHERE predicate;
```

显示所有索引或特定的索引名称的方法如下：
```roomsql
SHOW INDEX;
SHOW INDEX index_name;
```

通过索引名称删除索引的方法如下：
```roomsql
DROP INDEX index_name;
```

## 示例

路径配置白名单：["/tmp", "/opt/hetu", "/opt/openlookeng", "/etc/hetu", "/etc/openlookeng", 工作目录]

`etc` 目录包含config.properties，在指定config时，我们需要写绝对路径，该路径必须是白名单中路径的子目录

### 创建索引

``` shell
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute 'CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01", debugEnabled=true) WHERE p=part1'
```

### 显示索引

``` shell
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute "SHOW INDEX index_name"
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute "SHOW INDEX"
```

### 删除索引

``` shell
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute "DROP INDEX index_name"
```

*注意*: 删除索引命令不会将索引文件从hetu server的内存中清除。意味着索引会被继续使用，直到达到`hetu.heuristicindex.filter.cache.ttl`所设置的值或者hetu server被重启.


## 资源使用说明

### 内存

默认情况下，将使用默认的JVMMaxHeapSize(`java -XX:+PrintFlagsFinal -version | grep MaxHeapSize`)。为了提高性能，建议增大MaxHeapSize的取值。可以通过设置-Xmx值来实现：


``` shell
export JAVA_TOOL_OPTIONS="-Xmx100G"
```

在此示例中，MaxHeapSize被设置为100 GB。

### 磁盘使用
启发式索引使用本地临时文件夹存储创建的索引，然后打包上传至hdfs。因此，它需要临时文件夹（例如，linux上的`/tmp`)挂载的磁盘分区在本地有足够的可用空间。如果挂载的磁盘分区可用空间不足，用户可以在运行命令行时通过`-Djava.io.tmpdir`来指定使用的临时路径：
```bash
java -Djava.io.tmpdir=/path/to/another/dir -jar ./hetu-cli-*.jar
```

下面的公式给出了一个对于Bloom索引占用磁盘空间的大致估计。Bloom索引使用的空间大致与用于创建索引的表的大小成正比，同时与指定的`fpp`值的对数相反数成正比。因此，更小的fpp值和更大的数据集会使得创建的索引更大：

索引大小 = -log(fpp) * 表占用空间 * C

系数C还与其他许多因素相关，例如创建索引的列占表总数据的比重，但这些因素的影响应当不如fpp和表的大小重要，且变化较小。作为一个典型的拥有几个列的数据表，这个系数C在0.04左右。这就是说，为一个100GB的数据表的一列创建一个`fpp=0.001`的索引大致需要12GB磁盘空间，而创建`fpp=0.0001`的索引则需要16GB左右。

### 并行索引

如果在一台机器上为一个大表创建索引的速度太慢，则可以在不同的机器上并行为不同的分区创建索引。只要保证这些并行创建的分区不冲突即可。例如：

在机器1上：

``` bash
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute 'CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01") WHERE p=part1'
```

在机器2上：

``` shell
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute 'CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01") WHERE p=part2'
```
