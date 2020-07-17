
# JMX连接器

JMX连接器提供从openLooKeng集群中的所有节点查询JMX信息的能力。这对于检视或调试非常有用。Java管理扩展（JMX）提供了有关Java虚拟机和其中运行的所有软件的信息。openLooKeng本身就是通过JMX大量使用的工具。

还可以配置该连接器以便定期转储所选的JMX信息，并将其存储在内存中供以后访问。

## 配置

要配置JMX连接器，创建一个具有以下内容的目录属性文件`etc/catalog/jmx.properties`：

```{.none}
connector.name=jmx
```

要启用定期转储，请定义以下属性：

```{.none}
connector.name=jmx
jmx.dump-tables=java.lang:type=Runtime,presto.execution.scheduler:name=NodeScheduler
jmx.dump-period=10s
jmx.max-entries=86400
```

`dump-tables`是逗号分隔的托管Bean (MBean)列表。它指定了每个`dump-period`采样哪些MBean并存储在内存中。历史记录将限制为`max-entries`条条目的大小。`dump-period`和`max-entries`相应的默认值为`10s`和`86400`。

MBean名称中的逗号需要按如下方式进行转义：

```{.none}
connector.name=jmx
jmx.dump-tables=presto.memory:type=memorypool\\,name=general,\
   presto.memory:type=memorypool\\,name=system,\
   presto.memory:type=memorypool\\,name=reserved
```

## 查询JMX

JMX连接器提供了两种模式。

第一种是`current`，包含openLooKeng集群中每个节点的每个MBean。通过运行`SHOW TABLES`，可以看到所有可用的MBean：

    SHOW TABLES FROM jmx.current;

MBean名称映射到非标准表名，并且在查询中引用时必须用双引号引起来。例如，以下查询显示了每个节点的JVM版本：

    SELECT node, vmname, vmversion
    FROM jmx.current."java.lang:type=runtime";

```{.none}
node                 |              vmname               | vmversion
--------------------------------------+-----------------------------------+-----------
ddc4df17-0b8e-4843-bb14-1b8af1a7451a | Java HotSpot(TM) 64-Bit Server VM | 24.60-b09
(1 row)
```

以下查询显示了每个节点的打开和最大文件描述符计数：

    SELECT openfiledescriptorcount, maxfiledescriptorcount
    FROM jmx.current."java.lang:type=operatingsystem";

```{.none}
openfiledescriptorcount | maxfiledescriptorcount
-------------------------+------------------------
                    329 |                  10240
(1 row)
```

通配符`*`可以与`current`模式中的表名一起使用。这允许在单个查询中匹配多个MBean对象。以下查询返回每个节点上不同openLooKeng内存池的信息：

    SELECT freebytes, node, object_name
    FROM jmx.current."presto.memory:*type=memorypool*";

```{.none}
freebytes  |  node   |                       object_name
------------+---------+----------------------------------------------------------
 214748364 | example | presto.memory:type=MemoryPool,name=reserved
1073741825 | example | presto.memory:type=MemoryPool,name=general
 858993459 | example | presto.memory:type=MemoryPool,name=system
(3 rows)
```

`history`模式包含连接器属性文件中配置的表列表。这些表的列与当前模式中的列相同，但有一个额外的时间戳列存储快照的时间：

    SELECT "timestamp", "uptime" FROM jmx.history."java.lang:type=runtime";

```{.none}
timestamp        | uptime
-------------------------+--------
2016-01-28 10:18:50.000 |  11420
2016-01-28 10:19:00.000 |  21422
2016-01-28 10:19:10.000 |  31412
(3 rows)
```