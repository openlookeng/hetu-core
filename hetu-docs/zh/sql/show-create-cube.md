
SHOW CREATE CUBE
=================

概要
--------

``` sql
SHOW CREATE CUBE cube_name
```

描述
-----------

显示创建指定cube的SQL语句。

示例
--------

在`orders`表上创建cube`orders_cube`

    CREATE CUBE orders_cube ON orders WITH (AGGREGATIONS = (avg(totalprice), sum(totalprice), count(*)), 
    GROUP = (custKEY, ORDERkey), format= 'orc')

运行`SHOW CREATE CUBE`命令显示用于创建cube`orders_cube`的SQL语句：

    SHOW CREATE CUBE orders_cube;

``` sql
CREATE CUBE orders_cube ON orders WITH (AGGREGATIONS = (avg(totalprice), sum(totalprice), count(*)), 
GROUP = (custKEY, ORDERkey), format= 'orc')
```
