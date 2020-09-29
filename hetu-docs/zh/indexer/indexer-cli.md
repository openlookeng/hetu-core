
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

### 并行索引

如果在一台机器上为一个大表创建索引的速度太慢，则可以在不同的机器上并行为不同的分区创建索引。这需要设置parallelCreation标志并指定分区。例如：

在机器1上：

``` bash
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute 'CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01", parallelCreation=true) WHERE p=part1'
```

在机器2上：

``` shell
$ java -jar ./hetu-cli-*.jar --config /xxx/etc --execute 'CREATE INDEX index_name USING bloom ON hive.schema.table (column1) WITH ("bloom.fpp"="0.01", parallelCreation=true) WHERE p=part2'
```
