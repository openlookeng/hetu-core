+++

weight = 2
title = "Deploying openLooKeng Automatically"
+++

# Deploying openLooKeng Automatically



In addition to the manual deployment of openLooKeng Sever, you can follow below guide to complete deployment faster and easier. The script is friendly to most of linux OS. However, to Ubuntu, you need to manually install the following dependencies:

> sshpass1.06 or above

## Deploying openLooKeng on a Single Node

Execute below command can help you download the necessary packages and deploy openLooKeng server in one-click:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh)
```

or:

```shell
wget -O - https://download.openlookeng.io/install.sh|bash
```

Normally, you don\'t need to do any thing, except for the installation to complete. It will automatically start the service.

Execute below command to stop openLooKeng service.:

```shell
/opt/openlookeng/bin/stop.sh
```

Execute below command to start openLooKeng Command Line client:

```shell
/opt/openlookeng/bin/openlk-cli
```



## Deploying openLooKeng to Cluster online

Execute below command to install openLooKeng cluster:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -m
```

or:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) --multi-node
```

First of all, this command will download scripts and packages required by openLooKeng service. After the download is completed, it will check whether the dependent packages `expect` and `sshpass` are installed. If not, those dependencies will be installed automatically.

Besides, jdk version is required to be greater than 1.8.0\_151. If not, jdk1.8.0\_201 will be installed in the cluster. It is recommended to manually install these dependencies before installing openLooKeng service.

Secondly, the script will download openLooKeng-server tarball and copy that tarball to all the nodes in the cluster. Then install the openLooKeng-server by using this tarball.

Lastly, the script will setup openLooKeng server with the standard configurations, includes configurations for JVM, Node and also for build-in catalogs like `tpch`, `tpcds`, `memory connector`.

By design, the script will check if there are existing configuration under directory:
`/home/openlkadmin/.openlkadmin/cluster_node_info`

If this file is missing, the install script will ask user to input the nodes information.

Optionally, you can add user `openlkadmin` and create a file `/home/openlkadmin/.openlkconf/cluster_node_info`. 

In the `cluster_node_info`,  you should list appropriate values for your cluster.

Please refer to below template, and replace the variables denoted with brackets \<\> with actual values.

``` properties
COORDINATOR_IP=<master_node_ipaddress>
WORKER_NODES=<ip_address_1>[,<ip_address_2>,...,<ip_address_n>]
```

The general configurations for openLooKeng\'s coordinator, workers are taken from the configuration file
`/home/openlkadmin/.openlkadmin/cluster\_config\_info` and configurations for connectors are taken from the directory `/home/openlkadmin/.openlkadmin/catalog` respectively. If these directories or any required configuration files are absent during the deploy script running, default configuration files will be generated
automatically and deployed to all nodes.

Which means, alternatively, you can add those configuration files before running this deploy script, if you want to customized the deployment.

If above process all succeed, the deploy script will automatically start the openLooKeng Service for you. Execute below command to stop openLooKeng service.:

```shell
/opt/openlookeng/bin/stop.sh
```

Execute below command to start openLooKeng Command Line client.:

```shell
/opt/openlookeng/bin/openlk-cli
```



**Tips:** 

If you are going to deploy openLooKeng on a big cluster with lots of nodes, instead of inputting the nodes' IP address one by one. It is better to prepare a file containing all nodes' IP address then pass this file as parameter to the installation script. Here is the command:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -f <cluster_node_info_path>
```
or:
```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) --file <cluster_node_info_path>
```


For more help message,execute below command to deploy single node cluster:
```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -h
```   
or:
```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) --help
```



## Upgrade openLooKeng Service

Execute below command to Upgrade openLooKeng Service:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -u <version>
```

This command will upgrade the current openLooKeng Service to target version,
preserving all the existing configurations on current cluster. Execute
below command to list all available versions:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) -l
```

or:

```shell
bash <(wget -qO- https://download.openlookeng.io/install.sh) --list
```

## Deploying Configuration to openLooKeng Cluster

Modify configuration file (/home/openlkadmin/.openlkadmin/cluster\_config\_info) and then
execute below command to deploy the configurations to openLooKeng cluster:

```shell
bash /opt/openlookeng/bin/configuration_deploy.sh
```

Note, if you want to add more configrations or customize the configurations, you can add properties to the templates into file located at
`/home/openlkadmin/.openlkadmin/.etc_template/coordinator` 

or
`/home/openlkadmin/.openlkadmin/.etc_template/worker`. 

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

It is very easy and straight forward to uninstall openLooKeng Service, simply run below command:

```shell
bash /opt/openlookeng/bin/uninstall.sh
```

This will uninstall openLooKeng Service by removing directory `/opt/openlookeng` and all files inside it. However, the `openlkadmin` user and all the configuration files under`/home/openlkadmin/` will not be removed. If you wan to delete user and configuration files, you need to run the below command:

```shell
bash /opt/openlookeng/bin/uninstall.sh --all
```

## Deploying openLooKeng to Cluster offline

If you can't access the download URL from the machine where you want to install openLooKeng, you can download offline tarball and unpack to /opt directory.
Execute below command to deploy single node cluster:

```shell
bash /opt/openlookeng/bin/install_offline.sh`
```

Execute below command to deploy multi-node cluster:

```shell
bash /opt/openlookeng/bin/install_offline.sh -m
```

or:
```shell
bash /opt/openlookeng/bin/install_offline.sh --multi-node
```

execute the below command to get help on all available options:
```shell
bash /opt/openlookeng/bin/install_offline.sh --help
```

## Adding Node to Cluster 

If you want to add node to make the cluster bigger,execute the below command:

```shell
bash /opt/openlookeng/bin/add_cluster_node.sh -n
```

or:
```shell
bash /opt/openlookeng/bin/add_cluster_node.sh --node
```

or:
```shell
bash /opt/openlookeng/bin/add_cluster_node.sh -f <add_nodes_file_path>
```

or:
```shell
bash /opt/openlookeng/bin/add_cluster_node.sh --file <add_nodes_file_path>
```

If there are multiple nodes,separated by commas(,).

## Removing Node to Cluster 

If you want to remove node to make the cluster smaller,execute the below command:

```shell
bash /opt/openlookeng/bin/remove_cluster_node.sh -n
```

or:
```shell
bash /opt/openlookeng/bin/remove_cluster_node.sh --node
```

or:
```shell
bash /opt/openlookeng/bin/remove_cluster_node.sh -f <remove_nodes_file_path>
```

or:
```shell
bash /opt/openlookeng/bin/remove_cluster_node.sh --file <remove_nodes_file_path>
```

If there are multiple nodes, separate them with commas(,).

## See Also

[deployment](deployment.md)