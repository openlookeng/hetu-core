# 函数命名空间管理器

> **注意**
> 目前提供外部数据源函数(外部函数)的注册管理及下推的体验功能，此特性还在开发中，一些接口和配置项在后续的版本中可能会有改动。

## 函数命名空间管理器介绍

函数命名空间管理器是用来管理外部函数的，各个connector注册的外部函数(当前只支持标量函数)会在此空间管理。
一个函数命名空间表示为 `catalog.schema`(例如：`example.test`)，可以理解为函数定义存储的模式，不过此模式只能用于函数，不支持表或视图之类。
每个函数，不管是`built in`函数的还是外部注册的外部函数，都属于一个格式为`catalog.schema`的函数命名空间。
外部类型函数的全引用名为函数命名空间加上函数名(例如：`example.test.func`)。

所有`built in`函数的使用方式为直接使用函数名，如：`select count(*) from ...`。外部函数需要使用全引用名，如：`select example.default.format(...) from ...`。
每个函数命名空间管理器与一个`catalog`关联，并且管理与此`catalog`下的所有函数。
建议不使用实际`connector_catalog`名称作为函数命名空间管理器`catalog`。


## 加载函数命名空间管理器

要加载一个名称为`example`的函数命名空间管理器，我们可以添加一个名称为 `etc/function-namespace/example.properties`的文件, 同时，请在文件中写入如下配置内容:
``` properties
    function-namespace-manager.name=memory
    supported-function-languages=JDBC
```
当前仅支持类型为`memory`函数命名空间管理器，`memory`类型的函数命名空间管理器在系统重启后，需要重新加载外部函数信息。
配置项`supported-function-languages`表明当前函数命名空间管理器实例支持的外部函数种类，你可以自己定义注册类型，此项参数需要用户定义的外部函数注册信息一致。
当前我们仅支持`JDBC`。

## 加载多个函数命名空间管理器

我们可在`etc/function-namespace`目录下配置多个不同文件名文件来要加载多个函数命名空间管理器来管理不同的函数`catalog`。

## 外部函数注册到函数命名空间管理器

当前openLooKeng系统支持通过Jdbc Connector注册外部函数。具体请参考 [外部函数注册](../develop/externalfunction-registration-pushdown.md)

## 外部函数下推到数据源执行

openLooKeng系统支持外部函数通过Jdbc Connector下推到数据源执行。具体请参考 [外部函数注册](../develop/externalfunction-registration-pushdown.md)。
