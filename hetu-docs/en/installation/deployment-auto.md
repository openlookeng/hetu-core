
# Deploying openLooKeng Automatically



In addition to the manual deployment of openLooKeng Sever, you can follow the guide to complete deployment easily. The script is friendly to most of the Linux OS. However, to Ubuntu, you need to manually install the following dependencies:

> sshpass1.06 or above

## Deploying openLooKeng on a Single Node

Execute the following command can help you download the necessary packages and deploy openLooKeng server in one-click:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh)
```

or:

```shell
wget -O - https://download.openlookeng.io/install.sh|bash
```

You can complete the installation. The service starts automatically.

Execute the following command to stop openLooKeng service:

```shell
/opt/openlookeng/bin/stop.sh
```

Execute the following command to start openLooKeng Command Line client:

```shell
/opt/openlookeng/bin/openlk-cli
```



## Deploying openLooKeng to Cluster online

Execute the following command to install openLooKeng cluster:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -m
```

or:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) --multi-node
```

First of all, this command downloads scripts and packages required by openLooKeng service. After the download is completed, it will check whether the dependent packages `expect` and `sshpass` are installed. If not, those dependencies will be installed automatically.

Besides, jdk version is required to be greater than 1.8.0\_151. If not, jdk1.8.0\_201 will be installed in the cluster. It is recommended to manually install these dependencies before installing openLooKeng service.

Secondly, the script downloads openLooKeng-server tarball and copy that tarball to all the nodes in the cluster. Then install the openLooKeng-server by using this tarball.

Lastly, the script setup openLooKeng server with the standard configurations, includes configurations for JVM, Node and also for build-in catalogs such as `tpch`, `tpcds`, `memory connector`.

By design, the script will check if there are existing configuration under directory:
`/home/openlkadmin/.openlkadmin/cluster_node_info`

If this file is missing, the install script will ask user to input the nodes information.

Optionally, you can add user `openlkadmin` and create a file `/home/openlkadmin/.openlkconf/cluster_node_info`.

In the `cluster_node_info`,  you should list appropriate values for your cluster.

Refer to the following template, and replace the variables denoted with brackets \<\> with actual values.

``` properties
COORDINATOR_IP=<master_node_ipaddress>
WORKER_NODES=<ip_address_1>[,<ip_address_2>,...,<ip_address_n>]
```

The general configurations for openLooKeng\'s coordinator, workers are taken from the configuration file
`/home/openlkadmin/.openlkadmin/cluster_config_info` and configurations for connectors are taken from the directory `/home/openlkadmin/.openlkadmin/catalog` respectively. If these directories or any required configuration files are absent during the deploy script running, default configuration files will be generated automatically and deployed to all nodes.

This indicates that you can add those configuration files before running this deploy script alternatively, if you want to customize the deployment.

If above all process are succeed, then the deploy script will start the openLooKeng service automatically for you. Execute the following command to stop openLooKeng service:

```shell
/opt/openlookeng/bin/stop.sh
```

Execute the following command to start openLooKeng Command Line client:

```shell
/opt/openlookeng/bin/openlk-cli
```

**Tips**

If you are going to deploy openLooKeng on a big cluster with lots of nodes, instead of inputting the nodes' IP address one by one. It is better to prepare a file containing all nodes' IP address then pass this file as parameter to the installation script. Here is the command:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -f <cluster_node_info_path>
```
or:
```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) --file <cluster_node_info_path>
```


For more help message,execute the following command to deploy single node cluster:
```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -h
```
or:
```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) --help
```



## Upgrade openLooKeng Service

Execute the following command to Upgrade openLooKeng Service:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -u <version>
```

This command upgrades the current openLooKeng Service to target version,
preserving all the existing configurations on current cluster. Execute the following command to list all available versions:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -l
```

or:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) --list
```

## Deploying Configuration to openLooKeng Cluster

Modify configuration file (/home/openlkadmin/.openlkadmin/cluster\_config\_info) and then
execute the following command to deploy the configurations to openLooKeng cluster:

```shell
bash /opt/openlookeng/bin/configuration_deploy.sh
```

**Note**

*If you want to add more configurations or customize the configurations, you can add properties to the templates into file located at `/home/openlkadmin/.openlkadmin/.etc_template/coordinator` or `/home/openlkadmin/.openlkadmin/.etc_template/worker`.*

The property format has to be key=\<value\>, where value is wrapped with \'\<\' and \'\>\', which means it it a dynamic value. For example:

``` properties
http-server.http.port=<http-server.http.port>
exchange.client-threads=<exchange.client-threads>
```

Next, you need to add the actual value into configuration file `/home/openlkadmin/.openlkadmin/cluster_config_info`. For example:

``` properties
http-server.http.port=8090
exchange.client-threads=8
```

## Uninstall openLooKeng Service

It is very simple to uninstall openLooKeng Service, run the following command:

```shell
bash /opt/openlookeng/bin/uninstall.sh
```

This will uninstall openLooKeng Service by removing directory `/opt/openlookeng` and all files inside it. However, the `openlkadmin` user and all the configuration files under`/home/openlkadmin/` will not be removed. If you wan to delete user and configuration files, you need to run the following command:

```shell
bash /opt/openlookeng/bin/uninstall.sh --all
```

## Deploying openLooKeng to Cluster offline

If you cannot access the download URL from the machine where you want to install openLooKeng, you can download all required files beforehand and install offline.

1. Download `https://download.openlookeng.io/auto-install/openlookeng.tar.gz` and extract its content to `/opt`.

1. Create folder `/opt/openlookeng/resource` and save openLooKeng binary files under it: `https://download.openlookeng.io/<version>/hetu-server-<version>.tar.gz` and `https://download.openlookeng.io/<version>/hetu-cli-<version>-executable.jar`, where `<version>` refers to the version being installed, for example, `1.0.0`.

1. Save third party dependencies under `/opt/openlookeng/resource`. That is, download all files from either `https://download.openlookeng.io/auto-install/third-resource/x86/` or `https://download.openlookeng.io/auto-install/third-resource/aarch64/`, depending on the machine's architecture. This should include 1 `OpenJDK` file and 2 `sshpass` files.

1. If you plan to perform multi-node installation, and some nodes in the cluster have a different architecture type from the current machine, then also download the `OpenJDK` file for the other architecture, and save it under `/opt/openlookeng/resource/<arch>`, where `<arch>` is either `x86` or `aarch64`, corresponding to the other architecture.

After all resources are available, then execute the following command to deploy single node cluster:

```shell
bash /opt/openlookeng/bin/install_offline.sh
```

Execute the following command to deploy multi-node cluster:

```shell
bash /opt/openlookeng/bin/install_offline.sh -m
```

or:
```shell
bash /opt/openlookeng/bin/install_offline.sh --multi-node
```

Execute the following command to get help on all available options:

```shell
bash /opt/openlookeng/bin/install_offline.sh --help
```

## Adding Node to Cluster

If you want to add node to make the cluster bigger,execute the following command:

```shell
bash /opt/openlookeng/bin/add_cluster_node.sh -n <ip_address_1,……ip_address_N>
```

or:
```shell
bash /opt/openlookeng/bin/add_cluster_node.sh --node <ip_address_1,……ip_address_N>
```

or:
```shell
bash /opt/openlookeng/bin/add_cluster_node.sh -f <add_nodes_file_path>
```

or:
```shell
bash /opt/openlookeng/bin/add_cluster_node.sh --file <add_nodes_file_path>
```

If there are multiple nodes, separate them with commas(,). add_ nodes_ File example: ip_address_1,ip_address_2……,ip_address_N.

## Removing Node to Cluster

If you want to remove node to make the cluster smaller,execute the following command:

```shell
bash /opt/openlookeng/bin/remove_cluster_node.sh -n <ip_address_1,……ip_address_N>
```

or:
```shell
bash /opt/openlookeng/bin/remove_cluster_node.sh --node <ip_address_1,……ip_address_N>
```

or:
```shell
bash /opt/openlookeng/bin/remove_cluster_node.sh -f <remove_nodes_file_path>
```

or:
```shell
bash /opt/openlookeng/bin/remove_cluster_node.sh --file <remove_nodes_file_path>
```

If there are multiple nodes, separate them with commas(,). remove_ nodes_ File example: ip_address_1,ip_address_2……,ip_address_N.

## See Also

[Deploying openLooKeng Manually](./deployment.md)