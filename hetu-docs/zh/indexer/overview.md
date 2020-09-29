
# openLooKeng启发式索引

## 简介

可以在数据库表的一个或多个列创建索引，从而提供更快的随机查找。大多数大数据格式，如ORC、Parquet和CarbonData，都已经内置了索引。

启发式索引允许在现有数据上创建索引，但将索引存储在外部的的原始数据源中。这样做好处如下：

  - 索引对底层数据源不可知，并且可由任何查询引擎使用
  - 无需重写现有数据文件即可对现有数据进行索引
  - 可以创建底层数据源不支持的新索引类型
  - 索引数据不占用数据源存储空间

## 使用场景

当前，启发式索引支持ORC存储格式的hive数据源，帮助减少读取的分段或行数。

### 1.查询过程中过滤预定分段

当引擎需要调度一个TableScan操作时，它可以调度worker节点上的Split。这些Split负责读取部分源数据。但是如果应用了谓词，则并非所有Split都会返回数据。

通过为谓词列保留外部索引，启发式索引可以确定每个Split是否包含正在搜索的值，并且只对可能包含该值的Split安排读操作。

![indexer_filter_splits](../images/indexer_filter_splits.png)

### 2.读取ORC文件时提前筛选块

当需要从ORC文件中读取数据时，使用ORCRecordReader读取器。此读取器从数据条带批量（例如1024行）读取数据，然后形成页。但是，如果有一个谓词存在，那么就不需要批量读取中的所有条目，有些条目可能会在稍后被Filter运算符过滤掉。

通过为谓词列保留外部位图索引，启发式索引甚至可以在应用Filter运算符之前筛选出与谓词不匹配的行。

## 示例教程

这一教程将通过一个示例查询语句来展示索引的用法。

### 确定索引建立的列

对于这样的语句：

    SELECT * FROM table1 WHERE id="abcd1234";
   
如果id比较唯一，bloom索引可以大大较少读取的分段数量。

在本教程中我们将以这个语句为例。

### 配置索引

在 `etc/config.properties` 中加入这些行：

路径配置白名单：["/tmp", "/opt/hetu", "/opt/openlookeng", "/etc/hetu", "/etc/openlookeng", 工作目录]

注意：避免选择根目录；路径不能包含../；如果配置了node.data_dir,那么当前工作目录为node.data_dir的父目录；
    如果没有配置，那么当前工作目录为openlookeng server的目录

    hetu.heuristicindex.filter.enabled=true
    hetu.hetu.heuristicindex.filter.cache.max-memory=2GB
    hetu.heuristicindex.indexstore.uri=/opt/hetu/indices
    hetu.heuristicindex.indexstore.filesystem.profile=index-store-profile
    
然后在`etc/filesystem/index-store-profile.properties`中创建一个HDFS描述文件, 其中`index-store-profile`是上面使用的名字：

    fs.client.type=hdfs
    hdfs.config.resources=/path/to/core-site.xml,/path/to/hdfs-site.xml
    hdfs.authentication.type=NONE
    fs.hdfs.impl.disable.cache=true
    
如果HDFS集群开启了KERBEROS验证，还需要配置`hdfs.krb5.conf.path, hdfs.krb5.keytab.path, hdfs.krb5.principal`. 

在上面这个例子中使用了HDFS作为存储索引文件的位置，这使得索引可以在多个Hetu服务器间共享。如果要使用本地磁盘来存储索引，只要将 `index-store-profile.properties` 的内容改为:

    fs.client.type=local
    
注意：可以在`etc/filesystem`中配置多个不同名字的描述文件, 例如多个HDFS和一个Local，这样就可以通过改变`hetu.heuristicindex.indexstore.filesystem.profile`来在他们之中快速切换。

### 创建索引

要创建索引，首先将工作目录cd至安装目录的`bin`文件夹，然后运行：

    java -jar ./hetu-cli-*.jar --config <your-etc-folder-directory> --execute 'CREATE INDEX index_name USING bloom ON table1 (column)'
    
### 运行语句

完成上面的操作后，再次在Hetu服务器运行这个语句，服务器将在后台自动加载索引。在此期间如果反复执行同一语句，应该观察到分段的数量不断降低，最后降至一个很小的值。