
# 自动部署openLooKeng

除了手动部署openLooKeng服务器外，还可以按照以下指导更快、更容易地完成部署。这个脚本对大多数Linux操作系统都很友好。但是，对于Ubuntu，需要手动安装以下依赖项。

> sshpass1.06及以上版本
~~~shell
yum install sshpass
~~~

## 在单个节点上部署openLooKeng

执行以下命令可以帮助你一键下载所需的软件包和部署openLooKeng服务器：

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh)
```

或：

```shell
wget -O - https://download.openlookeng.io/install.sh|bash
```

通常情况下，你只需等待安装完成。服务会自动启动。

执行以下命令停止openLooKeng服务：

```shell
/opt/openlookeng/bin/stop.sh
```

执行以下命令启动openLooKeng命令行客户端。

```shell
/opt/openlookeng/bin/openlk-cli
```

## 将openLooKeng在线部署到集群

执行以下命令，按照提示分別输入coordinator、worker节点ip，安装openLooKeng多节点集群：

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -m
```

或：

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) --multi-node
```

首先，该命令将下载openLooKeng服务所需的脚本和包。下载完成后，会检查依赖包`expect`和`sshpass`是否安装。如果没有，这些依赖项将自动安装。

另外，jdk版本要求大于1.8.0\_151。如果未安装，则集群中会安装jdk1.8.0\_201。建议在安装openLooKeng服务之前手动安装这些依赖。

其次，该脚本将下载openLooKeng-server tarball，并将该tarball复制到集群中的所有节点。然后使用此tarball安装openLooKeng-server。

最后，该脚本将使用标准配置安装openLooKeng服务器，包括JVM、Node的配置以及`tpch`、`tpcds`、`memory connector`之类的内置目录的配置。

根据设计，脚本会检查`/home/openlkadmin/.openlkadmin/cluster_node_info`目录下是否有已有配置：

如果缺少该文件，安装脚本会要求用户输入节点信息。

或者，可以添加用户`openlkadmin`并创建文件`/home/openlkadmin/.openlkconf/cluster_node_info`。

在`cluster_node_info`中，你应该列出适合集群的值。

请参考以下模板，将中括号中的变量替换为实际值。

```properties
COORDINATOR_IP=<master_node_ipaddress>
WORKER_NODES=<ip_address_1>[,<ip_address_2>,...,<ip_address_n>]
```

openLooKeng的协调节点和工作节点的通用配置从配置文件`/home/openlkadmin/.openlkadmin/cluster_config_info`中获取，连接器的配置从目录`/home/openlkadmin/.openlkadmin/catalog`中获取。执行部署脚本时，如果缺少这些目录或者缺少必要的配置文件，则会自动生成默认配置文件，并部署到所有节点。

这意味着，如果希望自定义部署，也可以在运行此部署脚本之前添加这些配置文件。

如果上述过程全部成功，部署脚本将自动启动openLooKeng服务。执行以下命令停止openLooKeng服务：

```shell
/opt/openlookeng/bin/stop.sh
```

执行以下命令启动openLooKeng命令行客户端。

```shell
/opt/openlookeng/bin/openlk-cli
```

**提示：**

如果想将openLooKeng部署到一个节点较多的大集群中，而不逐个输入节点的IP地址，则最好准备一个包含所有节点IP地址的文件，然后将该文件作为参数传递给安装脚本。命令如下：

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -f <cluster_node_info_path>
```

或：

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) --file <cluster_node_info_path>
```

更多帮助信息，请执行以下命令部署单节点集群：

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -h
```

或：

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) --help
```

## openLooKeng服务升级

执行以下命令升级openLooKeng服务：

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -u <version>
```

此命令会将当前openLooKeng服务升级到目标版本，并保留当前集群上的所有现有配置。执行以下命令可以列出所有可用的版本：

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -l
```

或：

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) --list
```

## 将配置部署到openLooKeng集群

修改配置文件(/home/openlkadmin/.openlkadmin/cluster\_config\_info)，然后执行以下命令将配置部署到openLooKeng集群：

```shell
bash /opt/openlookeng/bin/configuration_deploy.sh
```

注意，如果想添加更多配置或自定义配置，可以将属性添加到模板存入到位于`/home/openlkadmin/.openlkadmin/.etc_template/coordinator`或`/home/openlkadmin/.openlkadmin/.etc_template/worker`的文件中。

属性的格式必须是key=\<value>，其中value用“\<”和“>”括起，意味着它是一个动态值。例如：

```properties
http-server.http.port=<http-server.http.port>
exchange.client-threads=<exchange.client-threads>
```

接下来，需要将实际值添加到配置文件`/home/openlkadmin/.openlkadmin/cluster_config_info`中。例如：

```properties
http-server.http.port=8090
exchange.client-threads=8
```

## 卸载openLooKeng服务

卸载openLooKeng Service非常简单直接，只需运行以下命令：

```shell
bash /opt/openlookeng/bin/uninstall.sh
```

这将通过移除删除目录`/opt/openlookeng`和其中所有文件来卸载openLooKeng服务。但不会删除`openlkadmin`用户及`/home/openlkadmin/`下的所有配置文件。如果要删除用户和配置文件，使用以下命令：

```shell
bash /opt/openlookeng/bin/uninstall.sh --all
```

## 将openLooKeng离线部署到集群

如果您无法从要安装openLooKeng的机器上访问下载URL，可以预先下载所需文件并执行离线安装。

1. 下载 `https://download.openlookeng.io/auto-install/openlookeng.tar.gz` 并将其内容解压到 `/opt` 目录。

1. 创建目录 `/opt/openlookeng/resource` 并保存 openLooKeng 执行文件 `https://download.openlookeng.io/<version>/hetu-server-<version>.tar.gz` 和 `https://download.openlookeng.io/<version>/hetu-cli-<version>-executable.jar`，其中`<version>`对应于正在安装的版本，例如`1.0.0`。

1. 同时将第三方依赖保存在 `/opt/openlookeng/resource` 目录下。根据本机的架构，下载 `https://download.openlookeng.io/auto-install/third-resource/x86/` 或 `https://download.openlookeng.io/auto-install/third-resource/aarch64/` 下面的全部文件。这应该包括一个 `OpenJDK` 文件和两个 `sshpass` 文件。

1. 如需要部署多节点，而且有些节点的架构与本机不同，还需要下载对应架构的 `OpenJDK` 文件并保存在 `/opt/openlookeng/resource/<arch>` 目录下，其中 `<arch>` 是 `x86` 或者 `aarch64`，对应于另一架构。

上面所有资源就位后，执行以下命令部署单节点集群：

```shell
bash /opt/openlookeng/bin/install_offline.sh
```

执行以下命令，按照提示分别输入coordinator、worker节点IP，部署多节点集群：

```shell
bash /opt/openlookeng/bin/install_offline.sh -m
```

或：

```shell
bash /opt/openlookeng/bin/install_offline.sh --multi-node
```

执行以下命令获得有关所有可用选项的帮助：

```shell
bash /opt/openlookeng/bin/install_offline.sh --help
```

## 向集群添加节点

如果想增加节点使集群规模更大，则执行以下命令：

```shell
bash /opt/openlookeng/bin/add_cluster_node.sh -n <ip_address_1,……ip_address_N>
```

或：

```shell
bash /opt/openlookeng/bin/add_cluster_node.sh --node <ip_address_1,……ip_address_N>
```

或：

```shell
bash /opt/openlookeng/bin/add_cluster_node.sh -f <add_nodes_file_path>
```

或：

```shell
bash /opt/openlookeng/bin/add_cluster_node.sh --file <add_nodes_file_path>
```

如果有多个节点，以逗号（,）分隔。add_nodes_file示例：ip_address_1,ip_address_2……,ip_address_N。

## 从集群移除节点

如果想移除节点使集群规模更小，则执行以下命令：

```shell
bash /opt/openlookeng/bin/remove_cluster_node.sh -n <ip_address_1,……ip_address_N>
```

或：

```shell
bash /opt/openlookeng/bin/remove_cluster_node.sh --node <ip_address_1,……ip_address_N>
```

或：

```shell
bash /opt/openlookeng/bin/remove_cluster_node.sh -f <remove_nodes_file_path>
```

或：

```shell
bash /opt/openlookeng/bin/remove_cluster_node.sh --file <remove_nodes_file_path>
```

如果有多个节点，以逗号（,）分隔。add_nodes_file示例：ip_address_1,ip_address_2……,ip_address_N。

## 参考资料

[手动部署openLooKeng](./deployment.md)