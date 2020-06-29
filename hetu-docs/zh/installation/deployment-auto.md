+++

weight = 2
title = "自动部署openLooKeng"
+++

# 自动部署openLooKeng


除了手动部署openLooKeng服务器外，还可以按照下面的指导更快、更容易地完成部署。这个脚本对大多数Linux操作系统都很友好。但是，对于Ubuntu，需要手动安装以下依赖项。

> expect-5.44及以上版本
> 
> sshpass1.06及以上版本

## 在单个节点上部署openLooKeng

执行以下命令可以帮助你一键下载所需的软件包和部署openLooKeng服务器：

    bash <(wget -qO- http://openlookeng.io/install.sh)

或：

    wget -O - http://openlookeng.io/install.sh|sh

通常情况下，你只需等待整个处理完成。服务会自动启动。

执行下面命令停止openLooKeng服务：

    /opt/hetu/bin/stop.sh

执行下面的命令启动openLooKeng命令行客户端。

    /opt/hetu/bin/hetu-cli

## 将openLooKeng在线部署到集群

执行下面命令安装openLooKeng集群：

    bash <(wget -qO- http://openlookeng.io/install.sh) -m

或：

    bash <(wget -qO- http://openlookeng.io/install.sh) --mutil-node

首先，该命令将下载openLooKeng服务所需的脚本和包。下载完成后，会检查依赖包`expect`和`sshpass`是否安装。如果没有，这些依赖项将自动安装。

另外，jdk版本要求大于1.8.0\_151。如果未安装，则集群中会安装jdk1.8.0\_201。建议在安装openLooKeng服务之前手动安装这些依赖。

其次，该脚本将下载openLooKeng-server tarball，并将该tarball复制到集群中的所有节点。然后使用此tarball安装openLooKeng-server。

最后，该脚本将使用标准配置安装openLooKeng服务器，包括JVM、Node的配置以及`tpch`、`tpcds`、`memory connector`之类的内置目录的配置。

根据设计，脚本会检查`/home/hetuadmin/.hetuadmin/cluster_node_info`目录下是否有已有配置：

如果缺少该文件，安装脚本会要求用户输入节点信息。

或者，可以添加用户`hetuadmin`并创建文件`/home/hetuadmin/.hetuconf/cluster_node_info`。

在`cluster_node_info`中，你应该列出适合集群的值。

请参考下面的模板，将中括号中的变量替换为实际值。

```{.none}
COORDINATOR_IP=<master_node_ipaddress>
WORKER_NODES=<ip_address_1>[,<ip_address_2>,...,<ip_address_n>]
```

openLooKeng的协调节点和工作节点的通用配置从配置文件`/home/hetuadmin/.hetuadmin/cluster\_config\_info`中获取，连接器的配置从目录`/home/hetuadmin/.hetuadmin/catalog`中获取。执行部署脚本时，如果缺少这些目录或者缺少必要的配置文件，则会自动生成默认配置文件，并部署到所有节点。

这意味着，如果希望自定义部署，也可以在运行此部署脚本之前添加这些配置文件。

如果上述过程全部成功，部署脚本将自动启动openLooKeng服务。执行下面命令停止openLooKeng服务：

    /opt/hetu/bin/stop.sh

执行下面的命令启动openLooKeng命令行客户端。

    /opt/hetu/bin/hetu-cli

**提示：**

如果想将openLooKeng部署到一个节点较多的大集群中，而不逐个输入节点的IP地址，则最好准备一个包含所有节点IP地址的文件，然后将该文件作为参数传递给安装脚本。命令如下：

`bash <(wget -qO- http://openlookeng.io/install.sh) -f <cluster_node_info_path>`或`bash <(wget -qO- http://openlookeng.io/install.sh) --file <cluster_node_info_path>`

更多帮助信息，请执行下面的命令部署单节点集群：`bash <(wget -qO- http://openlookeng.io/install.sh) -h`  
或`bash <(wget -qO- http://openlookeng.io/install.sh) --help`

## openLooKeng服务升级

执行下面命令升级openLooKeng服务：

    bash <(wget -qO- http://openlookeng.io/install.sh) -u <version>

此命令会将当前openLooKeng服务升级到目标版本，并保留当前集群上的所有现有配置。执行以下命令可以列出所有可用的版本：

    bash <(wget -qO- http://openlookeng.io/install.sh) -l

或：

    bash <(wget -qO- http://openlookeng.io/install.sh) --list

## 将配置部署到openLooKeng集群

修改配置文件\[/home/hetuadmin/.hetuadmin/cluster\_config\_info]{.title-ref}，然后执行以下命令将配置部署到openLooKeng集群：

    bash /opt/hetu/bin/configuration_deploy.sh

注意，如果想添加更多配置或自定义配置，可以将属性添加到模板存入到位于`/home/hetuadmin/.hetuadmin/.etc_template/coordinator`

或`/home/hetuadmin/.hetuadmin/.etc_template/worker`的文件中。

属性的格式必须是key=\<value>，其中key用'\<'和'>'括起，意味着它是一个动态值。例如：

```{.none}
http-server.http.port=<http-server.http.port>
exchange.client-threads=<exchange.client-threads>
```

接下来，需要将实际值添加到配置文件`/home/hetuadmin/.hetuadmin/cluster_config_info`中。例如：

```{.none}
http-server.http.port=8090
exchange.client-threads=8
```

## 卸载openLooKeng服务

卸载openLooKeng Service非常简单直接，只需运行以下命令：

    bash /opt/hetu/bin/cleanup.sh

这将通过移除删除目录`/opt/hetu`和其中所有文件来卸载openLooKeng服务。但不会删除`hetuadmin`用户及`/home/hetuadmin/`下的所有配置文件。如果要删除用户和配置文件，使用以下命令：

    bash /opt/hetu/bin/uninstall.sh --all

## 将openLooKeng离线部署到集群

如果你无法连接到我们的服务器，可以下载离线tarball并解压缩到**/opt**目录。执行以下命令部署单节点集群：`bash /opt/hetu/bin/install_offline.sh` 执行以下命令部署多节点集群：`bash /opt/hetu/bin/install_offline.sh -m` 或：`sh /opt/hetu/bin/install_offline.sh --multi-node` 更多帮助信息，请执行下面的命令部署单节点集群：`bash /opt/hetu/bin/install_offline.sh -m`  
或`bash /opt/hetu/bin/install_offline.sh --help`

## 参考资料

[部署](./deployment.md)