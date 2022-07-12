
# mpp连接器

本方案旨在通过GDS高速协议来提高openLookeng引擎读取GaussDB的性能。

目前对于GaussDB数据库的数据查询方式是通过JDBC Connector来实现的，和大部分关系型数据库连接类似。由于传统的MySQL等数据库存储的数据量小，因此通过JDBC方式来获取数据无可厚非。但是由于GaussDB是一种MPP类的分布式数据库，存储数据量大，且主要用于OLAP场景的分析，导致通过原生JDBC方式拉取数据的方式变得低效，因为它是一种单进单出的模式。

后来，社区也针对这种情况进行了基于JDBC的优化，例如增加了下推，在引擎端增加了多split并发，但是依然无法避免数据源端的单并发，性能瓶颈依然无法得到完全的解决，而且多并发还会带来多连接的情况，对数据库集群造成一定的压力。

本方案将会通过解决数据源端和引擎端并发问题来提高引擎查询的效率。

# mpp连接器设计思路

本方案将mpp类数据库的查询转换成对hive外表的查询，即将mpp类数据库的表数据快速导出到外部的分布式文件系统或分布式缓存，然后通过挂hive外表的方式通过hive connector来进行查询。本方案的主要权衡点在于导出数据的效率和mpp数据库通过单CN节点对外输出数据的效率的比较。随着查询数据量的递增，本方案的效率提高也会越来越高。
更详细的内容见社区分享：https://mp.weixin.qq.com/s/Q-t592UerICHNXI63rhtPg

## 配置

要配置mpp连接器，在`etc/catalog`中创建一个目录属性文件，例如`mpp.properties`，使用以下内容创建文件，并根据设置替换连接属性：

``` properties
本方案本质上是将查询gaussdb数据库转换成查询hive，因此以下配置均基于此原理。

connector.name=mpp
# 配置用来做最后查询的hive仓库
hive.metastore.uri=thrift://localhost:9083

etl-reuse=false #是否复用本次导数结果

#GDS baseinfo
#gds进程，基于postgres的fdw机制实现的一个快速导数进程，gaussdb官方插件
gds-list=gsfs://localhost:port1|base_path #gds的ip和端口，以及该进程启动时候的basepath，多个gds进程可以通过逗号分隔
aux-url=alluxio://localhost:19998 #alluxio的ip和端口
base-aux=/gdsdata/ #alluxio中用来为gds导出数据服务的路径，可自定义

#hive info
# 用来进行创建外表等操作的hive仓库连接配置
hive-user=username
hive-passwd=password
hive-db=xxx_db
hive-url=jdbc:hive2://localhost:10000/

# hive template
## 进行hive外表创建的相关SQL模板，一般无需更改
hsql-drop=drop table if exists ${table_name}
hsql-create=CREATE EXTERNAL TABLE ${table_name} ( ${schema_info} ) COMMENT 'gds external table' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' LOCATION '${pipe_to_aux_base_path}'

# gsDB connection info
# 要查询的GaussDB数据库相关连接信息
gs-driver=org.postgresql.Driver
gs-url=jdbc:postgresql://localhost:25308/schema
gs-user=user
gs-passwd=password

# gaussdb template
# 利用gds触发导数的相SQL模板，一般无需更改
gsql-create=create foreign table ${gaussdb_name}.ext_${table_name} (   ${schema_info} ) SERVER gsmpp_server OPTIONS ( LOCATION '${gds_foreign_location}',  FORMAT 'text',  DELIMITER E',', NULL '', encoding 'UTF-8', noescaping 'true', EOL E'\\n', out_filename_prefix '${table_name}') WRITE ONLY;
gsql-insert=insert into ${gaussdb_name}.ext_${table_name} select ${schema_info} from ${gaussdb_name}.${table_name};
gsql-drop=drop foreign table if exists ${gaussdb_name}.ext_${table_name};

```

## 使用

mpp连接器会将查询转换为对所配置的gaussdb的查询，因此如果你要查询opengauss.testdb.usertbl，如果想通过mpp 
connector进行快速查询，则可以写成：

    select * from mpp.testdb.usertbl;
 
如果您对mpp connector有更多的需求和见解，欢迎提issue和pr。