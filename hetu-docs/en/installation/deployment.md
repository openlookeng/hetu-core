
# Deploying openLooKeng Manually

This is a Manual deployment method, you can also use automatic deployment via script. (see [Deploying openLooKeng Automatically](./deployment-auto.md))

## Installing openLooKeng

Download the openLooKeng server tarball, and unpack it. The tarball will contain a single top-level directory, which we will call the *installation* directory.

openLooKeng needs a *data* directory for storing logs, etc. We recommend creating a data directory outside of the installation directory, which allows it to be easily preserved when upgrading openLooKeng.

## Configuring openLooKeng

Create an `etc` directory inside the installation directory. This will hold the following configuration:

-   Node Properties: environmental configuration specific to each node
-   JVM Config: command line options for the Java Virtual Machine
-   Config Properties: configuration for the openLooKeng server
-   Catalog Properties: configuration for [Connectors](../connector/_index.md) (data sources)

### Node Properties

The node properties file, `etc/node.properties`, contains configuration specific to each node. A *node* is a single installed instance of openLooKeng on a machine. This file is typically created by the deployment system when openLooKeng is first installed. The following is a minimal `etc/node.properties`:

``` properties
node.environment=openlookeng
node.launcher-log-file=/opt/openlookeng/hetu-server-1.1.0/log/launch.log
node.server-log-file=/opt/openlookeng/hetu-server-1.1.0/log/server.log
catalog.config-dir=/opt/openlookeng/hetu-server-1.1.0/etc/catalog
node.data-dir=/opt/openlookeng/hetu-server-1.1.0/data
plugin.dir=/opt/openlookeng/hetu-server-1.1.0/plugin
```

The above properties are described below:

- `node.environment`: The name of the environment. All openLooKeng nodes in a cluster must have the same environment name.

- `node.data-dir`: The location (filesystem path) of the data directory. openLooKeng will store logs and other data here.

- `node.launcher-log-file`: launch.log. This log is created by the launcher and is connected to the stdout and stderr streams of the server. It will contain a few log messages that occur while the server logging is being initialized and any errors or diagnostics produced by the JVM.

- `node.server-log-file`: server.log. This is the main log file used by openLooKeng. It will typically contain the relevant information if the server fails during initialization. It is automatically rotated and compressed.

- `catalog.config-dir`: openLooKeng accesses data via *connectors*, which are mounted in catalogs. Catalogs are registered by creating a catalog properties file in the `etc/catalog` directory.

- `plugin.dir`: The location  of the plugin directory.

  Note: The specific path is modified according to the actual installation path of openLooKeng. For example, the installation path of openLooKeng in the example is: /opt/openlookeng/hetu-server-1.1.0/.

### JVM Config

The JVM config file, `etc/jvm.config`, contains a list of command line options used for launching the Java Virtual Machine. The format of the file is a list of options, one per line. These options are not interpreted by the shell, so options containing spaces or other special characters should not be quoted.

The following provides a good starting point for creating `etc/jvm.config`:

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

The Xmx size in the parameter is 70% of the available memory of the server (recommended value, availableMem*70%).

Because an `OutOfMemoryError` will typically leave the JVM in an inconsistent state, we write a heap dump (for debugging) and forcibly terminate the process when this occurs.

### Config Properties

The config properties file, `etc/config.properties`, contains the configuration for the openLooKeng server. Every openLooKeng server can function as both a coordinator and a worker, but dedicating a single machine to only perform coordination work provides the best performance on larger clusters.

The following is a minimal configuration for the coordinator:

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

The following is a minimal configuration for the workers:

``` properties
coordinator=false
http-server.http.port=8080
query.max-memory=50GB
query.max-total-memory=50GB
query.max-memory-per-node=10GB
query.max-total-memory-per-node=10GB
discovery.uri=http://example.net:8080
```

Alternatively, if you are setting up a single machine for testing that will function as both a coordinator and worker, use this configuration:

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

These properties require some explanation:

-   `coordinator`: Allow this openLooKeng instance to function as a coordinator (accept queries from clients and manage query execution).
-   `node-scheduler.include-coordinator`: Allow scheduling work on the coordinator. For larger clusters, processing work on the coordinator can impact query performance because the machine\'s resources are not available for the critical task of scheduling, managing and monitoring query execution.
-   `http-server.http.port`: Specifies the port for the HTTP server. openLooKeng uses HTTP for all communication, internal and external.
-   `query.max-memory`: The maximum amount of distributed memory that a query may use. The parameter is N*query.max-memory-per-node, where N is the number of working nodes.
-   `query.max-memory-per-node`: The maximum amount of user memory that a query may use on any one machine. This parameter is 70% (recommended value) of Xmx in the JVM configuration.
-   `query.max-total-memory-per-node`: The maximum amount of user and system memory that a query may use on any one machine, where system memory is the memory used during execution by readers, writers, and network buffers, etc. This parameter is 70% (recommended value) of Xmx in the JVM configuration.
-   `discovery-server.enabled`: openLooKeng uses the Discovery service to find all the nodes in the cluster. Every openLooKeng instance will register itself with the Discovery service on startup. In order to simplify deployment and avoid running an additional service, the openLooKeng coordinator can run an embedded version of the Discovery service. It shares the HTTP server with openLooKeng and thus uses the same port.
-   `discovery.uri`: The URI to the Discovery server. Because we have enabled the embedded version of Discovery in the openLooKeng coordinator, this should be the URI of the openLooKeng coordinator. Replace `example.net:8080` to match the host and port of the openLooKeng coordinator. This URI must not end in a slash. For example, the openLooKeng coordinator ip is 127.0.0.1, the port is 8080, and discovery.uri=http://127.0.0.1:8080.

The following properties may be set:

-   `jmx.rmiregistry.port`: Specifies the port for the JMX RMI registry. JMX clients should connect to this port.
-   `jmx.rmiserver.port`: Specifies the port for the JMX RMI server. openLooKeng exports many metrics that are useful for monitoring via JMX.

See also [Resource Groups](../admin/resource-groups.md).

### Log Levels

The optional log levels file, `etc/log.properties`, allows setting the minimum log level for named logger hierarchies. Every logger has a name, which is typically the fully qualified name of the class that uses the
logger. Loggers have a hierarchy based on the dots in the name (like Java packages). For example, consider the following log levels file:

``` properties
io.prestosql=INFO
```

This would set the minimum level to `INFO` for both `io.prestosql.server` and `io.prestosql.plugin.hive`. The default minimum level is `INFO`. There are four levels: `DEBUG`, `INFO`, `WARN` and `ERROR`.

### Catalog Properties

openLooKeng accesses data via *connectors*, which are mounted in catalogs. The connector provides all of the schemas and tables inside of the catalog. For example, the Hive connector maps each Hive database to a schema, so if the Hive connector is mounted as the `hive` catalog, and Hive
contains a table `clicks` in database `web`, that table would be accessed in openLooKeng as `hive.web.clicks`.

Catalogs are registered by creating a catalog properties file in the `etc/catalog` directory. For example, create `etc/catalog/jmx.properties` with the following contents to mount the `jmx` connector as the `jmx` catalog:

``` properties
connector.name=jmx
```

See [Connectors](../connector/_index.md) for more information about configuring connectors.

## Running openLooKeng

The installation directory contains the launcher script in `bin/launcher`. openLooKeng can be started as a daemon by running the following:

``` shell
bin/launcher start
```

Alternatively, it can be run in the foreground, with the logs and other output being written to stdout/stderr (both streams should be captured if using a supervision system like daemontools):

``` shell
bin/launcher run
```

Run the launcher with `--help` to see the supported commands and command line options. In particular, the `--verbose` option is very useful for debugging the installation.

After launching, you can find the log files in `var/log`:

-   `launcher.log`: This log is created by the launcher and is connected to the stdout and stderr streams of the server. It will contain a few log messages that occur while the server logging is being initialized and any errors or diagnostics produced by the JVM.
-   `server.log`: This is the main log file used by openLooKeng. It will typically contain the relevant information if the server fails during initialization. It is automatically rotated and compressed.
-   `http-request.log`: This is the HTTP request log which contains every HTTP request received by the server. It is automatically rotated and compressed.

## See Also

[Deploying openLooKeng Automatically](./deployment-auto.md)
