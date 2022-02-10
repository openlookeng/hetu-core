# 入门指南

## 要求

* Mac OS X或Linux
* Java 8 Update 161或更高版本(8u161+)（64位）。同时支持Oracle JDK和OpenJDK。
* AArch64 ([Bisheng JDK 1.8.262 或更高版本](https://www.hikunpeng.com/developer/devkit/compiler?data=JDK))
* Maven 3.3.9+（用于构建）
* Python 2.4+（与启动器脚本一起运行）

## 构建openLooKeng Core

openLooKeng Core是一个标准Maven项目。在项目根目录下运行以下命令即可：

    ./mvnw clean install

在第一次构建中，Maven将从Internet下载所有依赖项并将其缓存在本地存储库(`~/.m2/repository`)中，这可能需要相当长的时间。后续的构建会更快。

openLooKeng Core有一组全面的单元测试，运行这些测试可能需要几分钟的时间。您可以在构建时禁用这些测试：

    ./mvnw clean install -DskipTests

## 在您的IDE中运行openLooKeng Core

### 概述

首次构建openLooKeng Core之后，您可以将该项目加载到您的IDE中并运行服务器。我们建议使用[IntelliJ IDEA](http://www.jetbrains.com/idea/)。由于openLooKeng是一个标准Maven项目，因此您可以使用根文件`pom.xml`将其导入到您的IDE中。在IntelliJ中，从“Quick Start”框中选择“Open Project”或从“File”菜单中选择“Open”，然后选择根文件 `pom.xml`。

在IntelliJ中打开该项目之后，仔细检查是否为该项目正确配置了Java SDK：

* 打开“File”菜单并选择“Project Structure”。
* 在“SDKs”部分中，确保选择1.8 JDK（如果不存在，则创建一个）。
* 在“Project”部分中，确保将“Project language level”设置为8.0，因为openLooKeng Core使用了一些Java 8语言功能。

openLooKeng Core附带的样例配置可以直接用于开发。使用以下选项创建运行配置：

* Main Class：`io.prestosql.server.PrestoServer`
* VM Options：`-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties`
* Working directory：`$MODULE_DIR$`
* Use classpath of module：`presto-main`

工作目录应该是`presto-main`子目录。在IntelliJ中，使用`$MODULE_DIR$`可以自动完成该设置。

在config.properties文件中，替换plugin.bundles为plugin.dir，并指向plugin目录

plugin.dir=../hetu-server/target/hetu-server-{VERSION}/plugin

此外，必须使用您的Hive元存储Thrift服务的位置对Hive插件进行配置。使用正确的主机和端口（如果您没有Hive元存储，则使用下面的值）替换`localhost:9083`，将以下内容添加到虚拟机选项列表中：

    -Dhive.metastore.uri=thrift://localhost:9083

### 为Hive或HDFS使用SOCKS

如果您的本地计算机无法直接访问Hive元存储或HDFS集群，则可以使用SSH端口转发进行访问。设置一个动态SOCKS代理，其中SSH侦听本地端口1080：

    ssh -v -N -D 1080 server

然后在虚拟机选项列表中添加以下内容：

    -Dhive.metastore.thrift.client.socks-proxy=localhost:1080

### 日志记录

审计日志功能默认关闭。要使用默认设置开启，需要修改`etc/event-listener.properties`中的配置，确保运行服务端的进程对审计日志目录有写权限。

### 运行CLI

启动CLI以连接到服务器并运行SQL查询：

    presto-cli/target/hetu-cli-*-executable.jar

查询集群中的节点信息：

    SELECT * FROM system.runtime.nodes;

在样例配置中，Hive Connector挂载在`hive`目录中，因此您可以运行以下查询来显示Hive数据库`default`中的表：

    SHOW TABLES FROM hive.default;

## 代码样式

我们建议您使用IntelliJ作为IDE。可以在[代码样式](https://github.com/airlift/codestyle)存储库中找到项目的代码样式模板，其中还提供了我们的通用编程和Java指南。除此之外，您还应遵守以下要求：

* 将文档源文件（包括目录文件和其他常规文档文件）中的各节按字母顺序进行排列。一般而言，如果周围的代码中已经存在这样的排序，则按字母顺序排列方法/变量/节。
* 在合适的时候使用Java 8流API。不过，请注意流实现的性能不佳，因此应避免在内循环或其他对性能敏感的节中使用流实现。
* 在抛出异常时对错误进行分类。例如，PrestoException接受错误代码作为参数：`PrestoException(HIVE_TOO_MANY_OPEN_PARTITIONS)`。您可以利用该分类来生成报告，以便能够监视各种故障的出现频率。
* 确保所有文件都有适当的许可证标头；您可以通过运行`mvn license:format`来生成许可证。
* 考虑使用字符串格式化（使用Java`Formatter`类进行printf样式格式化）：`format("Session property %s is invalid: %s", name, value)`（请注意，应始终以静态方式导入`format()`）。有时，如果只需要附加某些内容，可以考虑使用`+`运算符。
* 除非是普通表达式，否则避免使用三元运算符。
* 如果Airlift的`Assertions`类中有覆盖您的情况的断言，请使用该断言，而不是手动编写该断言。随着时间的推移，我们可能会转向更流畅的断言，如AssertJ。
* 在编写Git提交消息时，请遵循这些[准则](https://chris.beams.io/posts/git-commit/)。

## 构建Web UI

openLooKeng Web UI使用JSX和ES6编写，由多个React组件组成。该源代码被编译并打包成与浏览器兼容的Javascript，然后该Javascript被签入到openLooKeng Core源代码（在文件夹`dist`中）中。您必须安装[Node.js](https://nodejs.org/en/download/)和[Yarn](https://yarnpkg.com/en/)才能执行这些命令。要在更改之后更新该文件夹，只需运行以下命令：

    yarn --cwd presto-main/src/main/resources/webapp/src install

如果没有任何Javascript依赖项发生更改（即`package.json`未更改），则运行以下命令会更快：

    yarn --cwd presto-main/src/main/resources/webapp/src run package

为了简化迭代，您还可以在`watch`模式下运行，此时会在检测到源文件更改时自动重新编译：

    yarn --cwd presto-main/src/main/resources/webapp/src run watch

要快速进行迭代，只需在打包完成后在IntelliJ中重新构建项目。项目资源将被热重载，更改将在浏览器刷新时得到反映。