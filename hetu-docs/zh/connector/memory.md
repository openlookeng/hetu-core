
# 内存连接器

内存连接器将所有数据和元数据存储在工作节点上的RAM中，当openLooKeng重新启动时，数据和元数据都将被丢弃。

## 配置

要配置内存连接器，创建一个具有以下内容的目录属性文件`etc/catalog/memory.properties`：

``` properties
connector.name=memory
memory.max-data-per-node=128MB
```

`memory.max-data-per-node`定义了每个节点存储在该连接器中的页的内存限制（默认值为128 MB）。

## 示例

使用内存连接器创建表：

    CREATE TABLE memory.default.nation AS
    SELECT * from tpch.tiny.nation;

在内存连接器中向表中插入数据：

    INSERT INTO memory.default.nation
    SELECT * FROM tpch.tiny.nation;

从Memory连接器中选择：

    SELECT * FROM memory.default.nation;

删除表：

    DROP TABLE memory.default.nation;

## 内存连接器限制

> - `DROP TABLE`之后，内存没有立即释放。内存在下一次对内存连接器进行写访问后释放。
> - 当一个工作节点发生故障/重新启动时，存储在其内存中的所有数据将永远丢失。为了防止静默数据丢失，此连接器将在对此类损坏表的任何读访问上引发错误。
> - 在写内存表过程中，如果由于各种原因查询失败，表将处于未定义状态。此类表应该手动删除并重新创建。尝试从此类表中读取数据可能会失败或返回部分数据。
> - 当协调节点失败/重新启动时，有关表的所有元数据都将丢失，但表的数据仍将存在于工作节点上，但这些工作节点将无法访问。
> - 由于每个协调节点将具有不同的元数据，因此该连接器无法与多个协调节点正常工作。