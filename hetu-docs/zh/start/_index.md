+++

weight = 2
bookToc = false
title = "快速入门"
+++

# 快速入门
本文档旨在指导用户快速在本地机器上部署并启动openLooKeng服务，更详细的安装要求和部署方式请参考[安装指南](https://www.openlookeng.io/zh-cn/docs/docs/installation.html)

## 环境准备

* 操作系统选择 Linux
* 内存大于4G
* 机器能够连接互联网
* 端口号8090 未被占用



## 一键部署

运行如下命令即可启动安装部署：

```shell
wget -O - https://download.openlookeng.io/install.sh|sh
```

该命令从openLooKeng官网下载安装脚本并启动脚本运行。脚本在运行过程中，会自动从openLooKeng官网下载最新的安装包以及所依赖的组件。在下载完成后，会自动启动部署工作。整个过程不需要用户做额外的操作。

当用户看到如下日志，便可以认为openLooKeng 部署成功了。

![img](/images/BE670A8C-9EA4-461D-AD22-AF12849D72F0.png)



成功部署后，用户可以了解以下信息，有助于更好地使用openLooKeng服务 。

- 一键部署给openLooKeng 默认配置了以下几个内置数据源，供用户直接使用。

  - [tpcds](https://www.openlookeng.io/zh-cn/docs/docs/connector/tpcds.html)
  - [tpch](https://www.openlookeng.io/zh-cn/docs/docs/connector/tpch.html)
  - [memory](https://www.openlookeng.io/zh-cn/docs/docs/connector/memory.html)

- openLooKeng 的安装路径为`/opt/openlookeng`。用户可以在这里找到openLooKeng 的配置文件。关于配置文件以及配置项，你可以从[这里]( https://www.openlookeng.io/zh-cn/docs/docs/installation/deployment.html )了解到更多信息。

- 新的用户`openlkadmin`会被创建用于执行 openLooKeng 相关的操作，包括启动/停止openLooKeng 服务、扩展/减小集群规模等。

- 一键部署也为常用的管理指令提供了脚本，用户可以从这里找到`/opt/openlookeng/bin`。

- openLooKeng的运行日志存储于`/home/openlookeng/.openlkadmin/`

- 一键部署也提供了[命令行工具（cli)]( https://www.openlookeng.io/zh-cn/docs/docs/installation/cli.html )，用于连接openLooKeng 服务。



## 使用openLooKeng

用户可以用命令行工具(cli)连接openLooKeng服务进行数据检索和分析。 通过如下指令启动cli：

```shell
bash /opt/openlookeng/bin/openlk-cli 
```

![img](/images/cli.png)



在cli 中用户可以输入标准SQL 与openLooKeng 服务器端进行交互。

例如，用户查看当前系统有已经配置的[catalog]( https://www.openlookeng.io/docs/docs/overview/concepts.html ):

```sql
show catalogs;
```

![img](/images/catalogs.png)

查看有tpcds 有包含哪些[schema]( https://www.openlookeng.io/zh-cn/docs/docs/overview/concepts.html )：

```sql
show schemas from tpcds;
Schema
--------------------
information_schema
sf1
sf10
sf100
sf1000
sf10000
sf100000
sf300
sf3000
sf30000
tiny
(11 rows)
```

这里，tpcds 根据数据大小划分了不同的schema，sf后跟的数值越大数据量就越大。每个schema都包含相同的表。

查看sf1中所包含的数据表：

```sql
show tables from tpcds.sf1;
```

![image-20200629140454598](/images/image-20200629140454598.png)


用户可以选择表进行数据检索：

```sql
select c_customer_id, c_first_name, c_last_name from tpcds.sf1.customer limit 10;
```

![image-20200629141214172](/images/image-20200629141214172.png)

或者运行更加复杂的多表联合查询：

```sql
SELECT
     "sr_customer_sk" "ctr_customer_sk"
   , "sr_store_sk" "ctr_store_sk"
   , "sum"("sr_return_amt") "ctr_total_return"
   FROM
     store_returns
   , date_dim
   WHERE ("sr_returned_date_sk" = "d_date_sk")
      AND ("d_year" = 2000)
   GROUP BY "sr_customer_sk", "sr_store_sk"
```
![image-20200629141757336](/images/image-20200629141757336.png)

用户可以查阅 [openLooKeng语法文档](https://www.openlookeng.io/zh-cn/docs/docs/sql.html )，了解更多语法规则。

关于openLooKeng 的更多功能和特性，请查阅[用户指导手册](https://www.openlookeng.io/zh-cn/docs/docs/overview.html)


