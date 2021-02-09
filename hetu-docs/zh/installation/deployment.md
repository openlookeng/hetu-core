
# 手动部署openLooKeng


这是一种手动部署方法，你也可以通过脚本使用自动部署方法。（参见[自动部署openLooKeng](./deployment-auto.md)）

## 安装openLooKeng

下载openLooKeng服务器tarball并解压缩。tarball包含一个顶级目录，我们将其称为*安装*目录。

openLooKeng需要一个*数据*目录来存储日志等。我们建议在安装目录之外创建一个数据目录，这样在升级openLooKeng时可以很容易地保留数据目录。

## 配置openLooKeng

在安装目录下创建`etc`目录。目录下包括以下配置：

- 节点属性：每个节点特有的环境配置。
- JVM配置：Java虚拟机的命令行选项。
- 配置属性：openLooKeng服务器的配置。
- 目录属性：[连接器](../connector/_index.md)（数据源）配置。

### 节点属性

节点属性文件`etc/node.properties`包含每个节点特有的配置。*节点*是机器上已安装的openLooKeng的单个实例。该文件通常在首次安装openLooKeng时由部署系统创建。下面是最基本的`etc/node.properties`：

``` properties
node.environment=openlookeng
node.launcher-log-file=/opt/openlookeng/hetu-server-1.1.0/log/launch.log
node.server-log-file=/opt/openlookeng/hetu-server-1.1.0/log/server.log
catalog.config-dir=/opt/openlookeng/hetu-server-1.1.0/etc/catalog
node.data-dir=/opt/openlookeng/hetu-server-1.1.0/data
plugin.dir=/opt/openlookeng/hetu-server-1.1.0/plugin
```

上述属性说明如下：

- `node.environment`：环境名称。同一集群内所有openLooKeng节点的环境名称必须相同。

- `node.data-dir`：数据目录的位置（文件系统路径）。openLooKeng将在这里存储日志和其他数据。

- `node.launcher-log-file`: launch.log文件。此日志由启动器创建，连接到服务器的stdout和stderr流。此日志将包含一些在初始化服务器日志记录时发生的日志消息，以及JVM产生的任何错误或诊断信息。

- `node.server-log-file`: server.log文件。这是openLooKeng使用的主要日志文件。如果服务器在初始化期间失败，此日志通常会包含相关信息。此日志会自动轮转和压缩。

- `catalog.config-dir`: 创建目录属性文件来注册目录。openLooKeng通过连接器访问数据，连接器挂载在目录中。

- `plugin.dir`: plugin目录位置。

  注：具体路径根据openLooKeng实际安装路径修改，比如例子中openLooKeng的安装路径为：/opt/openlookeng/hetu-server-1.1.0/。

### JVM配置

JVM配置文件etc/jvm.config包含用于启动Java虚拟机的命令行选项列表。文件的格式是一个选项列表，每行一个选项。这些选项不由shell解释，因此包含空格或其他特殊字符的选项不应被引用。

### JVM配置

JVM配置文件`etc/jvm.config`包含用于启动Java虚拟机的命令行选项列表。文件的格式是一个选项列表，每行一个选项。这些选项不由shell解释，因此包含空格或其他特殊字符的选项不应被引用。

以下为创建`etc/jvm.config`提供了一个良好的起点：

``` properties
-server
-Xmx16G
-XX:-UseBiasedLocking
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+UseGCOverheadLimit
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
```

参数中Xmx大小为服务器可用内存的70%（建议值，availableMem*70%）。

由于`OutOfMemoryError`通常会使JVM处于不一致的状态，因此我们编写一个堆转储（用于调试），并在出现这种情况时强制终止进程。

### 配置属性

配置属性文件`etc/config.properties`包含openLooKeng服务器的配置。每个openLooKeng服务器都可以同时充当协调节点和工作节点，但是将一台机器专用于只执行协调工作可以在较大的集群上提供最佳性能。

协调器的最基本配置如下：

``` properties
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080
query.max-memory=50GB
query.max-total-memory=50GB
query.max-memory-per-node=10GB
query.max-total-memory-per-node=10GB
discovery-server.enabled=true
discovery.uri=http://example.net:8080
```

工作节点的最基本配置如下：

``` properties}
coordinator=false
http-server.http.port=8080
query.max-memory=50GB
query.max-total-memory=50GB
query.max-memory-per-node=10GB
query.max-total-memory-per-node=10GB
discovery.uri=http://example.net:8080
```

或者，如果你正在为测试而安装一台同时作为协调节点和工作节点的机器，请使用以下配置：

``` properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=50GB
query.max-total-memory=50GB
query.max-memory-per-node=10GB
query.max-total-memory-per-node=10GB
discovery-server.enabled=true
discovery.uri=http://example.net:8080
```

上述属性说明如下：

- `coordinator`：允许此openLooKeng实例充当协调节点（接受来自客户端的查询并管理查询执行）。
- `node-scheduler.include-coordinator`：允许在协调节点上调度工作。对于较大的集群，协调节点上的处理工作会影响查询性能，因为机器的资源不能用于调度、管理和监视查询执行的关键任务。当部署单节点环境时，该参数置为true。
- `http-server.http.port`：指定HTTP服务器的端口号。openLooKeng使用HTTP进行所有内部和外部通信。
- `query.max-memory`：查询可能使用的最大分布式内存量。该参数为N*query.max-memory-per-node，其中N为工作节点数量。
- `query.max-memory-per-node`：查询在任何一台机器上可能使用的最大用户内存量。该参数为JVM配置中Xmx的70%（建议值）。
- `query.max-total-memory-per-node`：一个查询在任意一台机器上可以使用的最大用户和系统内存量，其中系统内存是读取器、写入器和网络缓冲区等在执行期间使用的内存。该参数为JVM配置中Xmx的70%（建议值）。
- `discovery-server.enabled`：openLooKeng使用Discovery服务查找集群中的所有节点。每个openLooKeng实例在启动时都会将自己注册到Discovery服务。为了简化部署并避免运行额外的服务，openLooKeng协调节点可以运行Discovery服务的嵌入式版本。该版本与openLooKeng共用HTTP服务器，因此使用相同端口。
- `discovery.uri`：Discovery服务器的URI。由于我们已经在openLooKeng协调节点中启用了Discovery的嵌入式版本，因此这应该是openLooKeng协调节点的URI。替换`example.net:8080`以匹配openLooKeng协调器的主机和端口。该URI不能以斜杠结尾。例如openLooKeng协调器ip为127.0.0.1，port为8080，discovery.uri=http://127.0.0.1:8080。

可以设置以下属性：

- `jmx.rmiregistry.port`：JMX RMI注册表端口。JMX客户端应该连接到此端口。
- `jmx.rmiserver.port`：JMX RMI服务器端口。openLooKeng导出了许多对通过JMX进行监视的有用指标。

另见[资源组](../admin/resource-groups.md)。

### 日志级别

可选日志级别文件`etc/log.properties`允许为命名日志记录器层次结构设置最低日志级别。每个记录器都有一个名称，通常是使用记录器的类的完全限定名。记录器具有基于名称中的点层次结构（如Java包）。例如，考虑如下日志级别文件：

``` properties
io.prestosql=INFO
```

这将把`io.prestosql.core.server`和`io.prestosql.core.plugin.hive`最低级别设置为`INFO`。默认的最低级别是`INFO`。有四个级别：`DEBUG`、`INFO`、`WARN`和`ERROR`。

### 目录属性

openLooKeng通过*连接器*访问数据，这些连接器安装在目录中。连接器提供了目录中的所有模式和表。例如，Hive连接器将每个Hive数据库映射到一个模式，所以如果Hive连接器作为`hive`目录挂载，并且Hive包含数据库`web`中的表`clicks`，那么该表将在openLooKeng中作为`hive.web.clicks`访问。

通过在`etc/catalog`目录中创建目录属性文件来注册目录。例如，用以下内容创建`etc/catalog/jmx.properties`，将`jmx`连接器挂载为`jmx`目录：

``` properties
connector.name=jmx
```

有关配置连接器的详细信息，请参阅[连接器](../connector/_index.md)。

## 运行openLooKeng

安装目录中在`bin/launcher`中包含启动器脚本。openLooKeng可以通过运行如下命令以守护进程的方式启动：

``` shell
bin/launcher start
```

也可以在前台运行，将日志和其他输出写入stdout/stderr（如果使用像守护进程这样的监控系统，应该捕捉两个流）：

``` shell
bin/launcher run
```

带`--help`运行启动器，查看支持的命令和命令行选项。特别是，`--verbose`选项对于调试安装非常有用。

启动后，在`var/log`路径中可以找到日志文件：

- `launcher.log`：此日志由启动器创建，连接到服务器的stdout和stderr流。此日志将包含一些在初始化服务器日志记录时发生的日志消息，以及JVM产生的任何错误或诊断信息。
- `server.log`：这是openLooKeng使用的主要日志文件。如果服务器在初始化期间失败，此日志通常会包含相关信息。此日志会自动轮转和压缩。
- `http-request.log`：这是HTTP请求日志，包含服务器接收到的每个HTTP请求。此日志会自动轮转和压缩。

## 参考资料

[自动部署openLooKeng](./deployment-auto.md)