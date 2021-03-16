Function Namespace Managers
===========================

> **Warn**
> This feature is developing now. So some interfaces and configuration may be changed in the next version.

Introduction
------------
Function Namespace Managers support storing `external function`, and the `external functions` which register from connectors will be stored in it.
A function namespace is in the format of, `catalog.schema`(For example:`example.test`). It is only a schema for storing function, but not for storing table and view. 
Every function in openLooKeng, no matter `built in function` or `external function`, belongs to a function namespace `catalog.schema`.
The `built in function` belong to `presto.default`. The `external function` belong to a function namespace which is supplied by the uer, for example:`example.test`.
All of the `built in function` must be used with the function namespace omitted, for example:`select count(*) from ...`.
The `external function` must be used with a full qualified name, for example: `select example.default.format(...) from ...`.

Every instance of Function Namespace Manager related to a `catalog`, and manage all the function qualified by it.
We suggest that do not use a same name as the `Connector catalog`.


Mount Function Namespace Manager Instance
------------------
For example ,we want to mount a Function Namespace Manager Instance named `example`, we can add a property file named `etc/function-namespace/example.properties` which following contents:

``` properties
    function-namespace-manager.name=memory
    supported-function-languages=JDBC
```
Now we only support Function Namespace Manager named `memory`. The function information stored in the manager will lose after we restart the openLooKeng. 
The configuration property`supported-function-languages` declare function kind that the function manager support. Now we only support `JDBC`. 

Mount Multiple Function Namespace Manager Instances
---------------------
We can add different property file to mount multiple function namespace managers to manage different `catalog`.

Register External Functions to Function Namespace Manager
--------------------------
Now openLooKeng support register `external function` from Jdbc Connector which extends from presto-base-jdbc.
For more information, please refer to: [External Function Registration and Push Down](../develop/externalfunction-registration-pushdown.md)

External Function Push Down
-------------------
Now openLooKeng support to push `external function` down to data source.
For more information, please refer to: [External Function Registration and Push Down](../develop/externalfunction-registration-pushdown.md)。
