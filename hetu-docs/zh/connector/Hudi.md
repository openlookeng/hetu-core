# Hudi连接器

### Hudi介绍

Apache Hudi是一个快速迭代的数据湖存储系统，可以帮助企业构建和管理PB级数据湖。它提供在DFS上存储超大规模数据集，同时使得流式处理如果批处理一样，该实现主要是通过如下两个原语实现。

- **Update/Delete记录**: Hudi支持更新/删除记录，使用文件/记录级别索引，同时对写操作提供事务保证。查询可获取最新提交的快照来产生结果。（在文件中随机存取记录，记录的地址即为文件/记录索引）
- **Change Streams**: Hudi也支持增量获取表中所有更新/插入/删除的记录，从指定时间点开始进行增量查询。

### 支持的表格类型和查询类型

**Hudi支持如下两种类型表**

**Copy On Write (COW)**: 使用列式存储格式（如parquet）存储数据，在写入时同步更新版本/重写数据。

**Merge On Read (MOR)**: 使用列式存储格式（如parquet）+ 行存（如Avro）存储数据。更新被增量写入delta文件，后续会进行同步/异步压缩产生新的列式文件版本。

**Hudi支持如下查询类型**

**快照查询**: 查询给定commit/compaction的表的最新快照。对于Merge-On-Read表，通过合并基础文件和增量文件来提供近实时数据（分钟级）；对于Copy-On-Write表，对现有Parquet表提供了一个可插拔替换，同时提供了upsert/delete和其他特性。

**读优化查询**: 查询给定commit/compaction的表的最新快照。只提供最新版本的基础/列式数据文件，并可保证与非Hudi表相同的列式查询性能。

## 配置

通常情况下，我们会从外部数据源汲取数据并写入到DFS文件存储系统中，例如HDFS上，然后我们会通过Hudi提供的数据同步工具，将Hudi数据从HDFS上同步到Hive表中，此时Hudi表将作为Hive的外表存储，然后我们可以通过连接Hive Connector来访问Hudi表进行数据查询。Hudi表的生成以及同步到Hive表中的步骤请参考[Hudi官方样例](https://hudi.apache.org/docs/docker_demo.html)。

首先，在开始使用Hudi连接器之前，应该先完成以下步骤：

- 用于连接Hudi的JDBC连接详情

要配置Hudi连接器（即Hive 连接器），用以下内容创建`etc/catalog/hive.properties`，以将`hive-hadoop2`连接器挂载为`hive`目录，将`example.net:9083`替换为Hive元存储Thrift服务的正确主机和端口：

``` properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://example.net:9083
```

- 添加Hudi驱动 （Hudi版本为0.7.0）

```properties
# 下载Hudi源码并编译
cd /opt
wget https://github.com/apache/hudi/releases/tag/release-0.7.0
tar -zxvf hudi-release-0.7.0.tar.gz
cd hudi-release-0.7.0
mvn package -DskipTests
```

编译完成后，进入目录/opt/hudi-release-0.7.0/packaging/hudi-presto-bundle/target/，得到文件hudi-presto-bundle-0.7.0.jar，我们需要将其放到openLooKeng插件文件夹中。例如，openLooKeng插件包文件夹为 **/opt/hetu-server-1.3.0-SNAPSHOT/plugin**，则拷贝命令如下：**cp /opt/hudi-presto-bundle-0.7.0.jar /opt/hetu-server-1.3.0-SNAPSHOT/plugin/hive-hadoop2**。此时，为防止读取Hudi表时版本依赖的冲突，需要将当前目录下的hudi-common-0.8.0.jar, hudi-hadoop-mr-0.8.0.jar删除，还需要在当前目录下放入jackson-mapper-asl-1.9.13.jar和jackson-core-asl-1.9.13.jar文件（这两个jar包可以从Hudi源码编译后的/opt/hudi-release-0.7.0/hudi-cli/target/lib路径下找到）。

```properties
# 复制Hudi驱动包
cp hudi-presto-bundle-0.7.0.jar /opt/hetu-server-1.3.0-SNAPSHOT/plugin/hive-hadoop2
# 删除hudi-common-0.8.0.jar, hudi-hadoop-mr-0.8.0.jar包
cd /opt/hetu-server-1.3.0-SNAPSHOT/plugin/hive-hadoop2
rm -rf hudi-common-0.8.0.jar hudi-hadoop-mr-0.8.0.jar
# 从Hudi源码编译后的文件中找到jackson-mapper-asl-1.9.13.jar和jackson-core-asl-1.9.13.jar，并放入当前文件夹中
cp /opt/hudi-release-0.7.0/hudi-cli/target/lib/jackson-mapper-asl-1.9.13.jar .
cp /opt/hudi-release-0.7.0/hudi-cli/target/lib/jackson-core-asl-1.9.13.jar .
```

## 查询Hudi COW表

参考[Hudi官方样例](https://hudi.apache.org/docs/docker_demo.html)生成Hudi COW表（**stock_ticks_cow**）后，我们可以使用如下语句来进行查询

    # COPY-ON-WRITE Queries: 
    select "_hoodie_commit_time", symbol, ts, volume, open, close from stock_ticks_cow where symbol = 'GOOG';

当前查询结果（**快照查询**）：

    lk:default> select "_hoodie_commit_time", symbol, ts, volume, open, close from stock_ticks_cow where symbol = 'GOOG';
    _hoodie_commit_time | symbol |        ts          | volume | open    | close
    --------------------+--------+--------------------+--------+---------+---------
      20210519083634    | GOOG   | 2018-08-31 09:59:00|  6330  |1230.5   | 1230.02
      20210519083834    | GOOG   | 2018-08-31 10:59:00|  3391  |1227.1993| 1227.215
    (2 rows)

从当前的快照查询可以知道的是，最新的ts时间是2018-08-31 10:59:00，此时已经是最近更新后的数据

## 查询Hudi MOR表

参考[Hudi官方样例](https://hudi.apache.org/docs/docker_demo.html)生成Hudi MOR表（**stock_ticks_mor_ro**）后，我们可以使用如下语句来进行查询

    # Merge-On-Read Queries:
    select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
    

当前查询结果（**读优化查询**）：

    lk:default> select "_hoodie_commit_time", symbol, ts, volume, open, close from stock_ticks_mor_ro where symbol = 'GOOG';
    _hoodie_commit_time | symbol |        ts          | volume | open    | close
    --------------------+--------+--------------------+--------+---------+---------
      20210519083634    | GOOG   | 2018-08-31 09:59:00|  6330  |1230.5   | 1230.02
      20210519083634    | GOOG   | 2018-08-31 10:29:00|  3391  |1230.1899| 1230.085
    (2 rows)

从当前的读优化查询可以知道的是，最新的ts时间是2018-08-31 10:29:00，这个数据是从最基础的列式文件中读取得到的结果。

参考[Hudi官方样例](https://hudi.apache.org/docs/docker_demo.html)生成Hudi MOR表（**stock_ticks_mor_rt**）后，我们可以使用如下语句来进行查询

    # Merge-On-Read Queries:
    select "_hoodie_commit_time", symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';

当前查询结果（**快照查询**）：

    lk:default> select "_hoodie_commit_time", symbol, ts, volume, open, close from stock_ticks_mor_rt where symbol = 'GOOG';
    _hoodie_commit_time | symbol |        ts          | volume | open    | close
    --------------------+--------+--------------------+--------+---------+---------
      20210519083634    | GOOG   | 2018-08-31 09:59:00|  6330  |1230.5   | 1230.02
      20210519083834    | GOOG   | 2018-08-31 10:59:00|  3391  |1227.1993| 1227.215
    (2 rows)

从当前的快照查询可以知道的是，最新的ts时间是2018-08-31 10:59:00，此时已经是最近更新后的数据，这说明我们已经通过合并基础文件和增量文件来提供近实时数据

## Hudi连接器限制

- 暂时只支持Hudi COW表的快照查询
- 暂时只支持Hudi MOR表的快照查询和读优化查询