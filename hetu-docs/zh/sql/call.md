
# CALL

## 摘要

``` sql
CALL procedure_name ( [ name => ] expression [, ...] )
```

## 说明

调用过程。

过程可以由连接器提供，以执行数据操作或管理任务。例如，`/connector/system`定义了用于终止正在运行的查询的过程。

某些连接器（如 [PostgreSQL连接器](../connector/postgresql.md)）用于具有其自己的存储过程的系统。这些存储过程与此处讨论的连接器定义过程相分离，因此无法通过 `CALL` 直接调用。

有关可用过程的详细信息，请参见连接器文档。

## 示例

使用位置参数调用过程：

    CALL test(123, 'apple');

使用命名参数调用过程：

    CALL test(name => 'apple', id => 123);

使用完全限定名称调用过程：

    CALL catalog.schema.test();