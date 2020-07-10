+++
weight = 2
title = "索引命令行接口"
+++

# 索引命令行接口

##  用法

可执行索引文件在安装中位于`bin`目录下，例如`<安装路径>/bin/index`。因为默认使用相对路径，它必须在`bin`目录下运行。

```
使用方法：index [-v] [--debug] [--disableLocking] --table=<table>
         [-c=<configDirPath>] [--column=<columns>[,<columns>...]]...
         [--partition=<partitions>[,<partitions>...]]...
         [--type=<indexTypes>[,<indexTypes>...]]...[-p=<plugins>[,
         <plugins>...]]...<command>

使用此索引工具，您可以创建、显示和删除索引。

支持的索引类型如下：BITMAP, BLOOM, MINMAX

支持的索引存储：LOCAL, HDFS (必须 在{--config}/config.properties配置

支持的数据源：HIVE using ORC files (必须在{--config}/catalog/catalog_name.properties配置


      <command>          命令类型，如create、delete、show等；说明：delete命令只作用于列级。
                           
      --column=<columns>[,<columns>...]
                         列，使用逗号分隔多个列
      --debug            如果启用，则每个Split的原始数据也将随索引一起写入文件
                           
      --disableLocking   默认锁定在表级别启用；如果设置为false，用户必须确保相同的数据没有被多个调用方同时索引（允许不同列或分区并行索引）
    
      --partition=<partitions>[,<partitions>...]
                         只为这些分区创建索引，用逗号分隔多个分区
                           
      --table=<table>     全量表名
      --type=<indexTypes>[,<indexTypes>...]
                         索引类型，用逗号分隔多种类型 (支持的类型：BLOOM, BITMAP, MINMAX
  -c, --config=<configDirPath>
                         openLooKeng etc目录的根目录（默认为../etc）
  -p, --plugins=<plugins>[,<plugins>...]
                         plugins目录或文件(默认为. /hetu-heuristic-index/plugins）
                           
  -v                     verbose
```

## 示例

### 创建索引

``` shell
$ ./index -v -c ../etc --table hive.schema.table --column column1,column2 --type bloom,minmax,bitmap --partition p=part1 create
```

### 显示索引

``` shell
$ ./index -v -c ../etc --table hive.schema.table show
```

### 删除索引

*注意：*索引只能在表或列级别删除，即所有索引类型都将被删除。

``` shell
$ ./index -v -c ../etc --table hive.schema.table --column column1 delete
```

## 资源使用说明

### 内存

默认情况下，将使用默认的JVMMaxHeapSize(`java -XX:+PrintFlagsFinal -version | grep MaxHeapSize`)。为了提高性能，建议增大MaxHeapSize的取值。可以通过设置-Xmx值来实现：


``` shell
export JAVA_TOOL_OPTIONS="-Xmx100G"
```

在此示例中，MaxHeapSize被设置为100 GB。

###并行索引

如果在一台机器上为一个大表创建索引的速度太慢，则可以在不同的机器上并行为不同的分区创建索引。这需要设置--disableLocking标志并指定分区。例如：

在机器1上：

``` bash
$ ./index -v ---disableLocking c ../etc --table hive.schema.table --columncolumn1,column2 --type bloom,minmax,bitmap --partition p=part1 create
```

在机器2上：

``` shell
$ ./index -v ---disableLocking c ../etc --table hive.schema.table --columncolumn1,column2 --type bloom,minmax,bitmap --partition p=part2 create
```
