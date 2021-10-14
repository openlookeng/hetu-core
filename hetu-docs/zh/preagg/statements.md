# 用法
可以使用任何受支持的客户端管理Cubes，例如位于安装中`bin`目录下的hetu-cli。

## CREATE CUBE
### 概要

``` sql
CREATE CUBE [ IF NOT EXISTS ]
cube_name ON table_name WITH (
   AGGREGATIONS = ( expression [, ...] ),
   GROUP = ( column_name [, ...])
   [, FILTER = (expression)]
   [, ( property_name = expression [, ...] ) ] 
)
[WHERE predicate]
```
### 描述
使用指定的组和聚合创建一个新的空Cube。使用`INSERT INTO CUBE (see below)`来插入数据。

如果Cube已经存在，可选的`IF NOT EXISTS`子句会导致错误被抑制。
可选的`property_name`部分可用于在新创建的Cube上设置属性。

要列出所有可用的表属性，请运行以下查询：

    SELECT * FROM system.metadata.table_properties

**注意：** 这些属性仅限于为其创建Cube的连接器。

### 例子
在`orders`上创建一个新的Cube`orders_cube`：

    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), AVG(totalprice) ),
      GROUP = ( orderstatus, orderdate ),
      format = 'ORC'
    )

创建一个新的分区Cube`orders_cube`：

    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), AVG(totalprice) ),
      GROUP = ( orderstatus, orderdate ),
      format = 'ORC',
      partitioned_by = ARRAY['orderdate']
    )

使用一些源数据过滤器创建一个新的Cube`orders_cube`：

    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), COUNT DISTINCT(orderid) ),
      GROUP = ( orderstatus ),
      FILTER = (orderdate BETWEEN 2512450 AND 2512460)
    )

创建一个新的Cube`orders_cube`，并在Cube列上添加一些额外的谓词：

    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), COUNT DISTINCT(orderid) ),
      GROUP = ( orderstatus ),
      FILTER = (orderdate BETWEEN 2512450 AND 2512460)
    ) WHERE orderstatus = 'PENDING';

这与以下内容相同：
    
    CREATE CUBE orders_cube ON orders WITH (
      AGGREGATIONS = ( SUM(totalprice), COUNT DISTINCT(orderid) ),
      GROUP = ( orderstatus ),
      FILTER = (orderdate BETWEEN 2512450 AND 2512460)
    );
    INSERT INTO CUBE orders_cube WHERE orderstatus = 'PENDING';

`FILTER`属性可用于在构建Cube时从源表中过滤掉数据。
在对源表应用`orderdate BETWEEN 2512450 AND 2512460`谓词后，Cube建立在数据上。过滤谓词中使用的列不得属于Cube。

### 限制
- 可以仅使用以下聚合函数创建Cubes。
  换句话说，使用以下函数的查询只能使用Cubes进行优化。
  **COUNT, COUNT DISTINCT, MIN, MAX, SUM, AVG**
- 不同的连接器可能支持不同的数据类型和不同的表/列属性。

## INSERT INTO CUBE

### 概要
``` sql
INSERT INTO CUBE cube_name [WHERE condition]
```

### 描述
`CREATE CUBE`语句创建没有任何数据的Cube。要将数据插入Cube，请使用`INSERT INTO CUBE`SQL。
`WHERE`子句是可选的。如果提供了谓词，则只有与给定谓词匹配的数据才会从源表中处理并插入到Cube中。
否则，源表中的整个数据将被处理并插入到Cube中。

### 例子
将数据插入`orders_cube`Cube:

    INSERT INTO CUBE orders_cube WHERE orderdate > date '1999-01-01';
    INSERT INTO CUBE order_all_cube;

### 限制
1. 对同一个Cube的后续插入需要使用相同的列集

```sql
   CREATE CUBE orders_cube ON orders WITH (AGGREGATIONS = (count(*)), GROUP = (orderdate));
   
   INSERT INTO CUBE orders_cube WHERE orderdate BETWEEN date '1999-01-01' AND date '1999-01-05';
   
   -- This statement would fail because its possible the Cube already contain rows matching the given predicate.
   INSERT INTO CUBE orders_cube WHERE location = 'Canada';
```
**注意：** 这意味着在第一个插入中使用的列必须在第一个插入后的每个插入谓词中使用，以避免插入重复数据。

## INSERT OVERWRITE CUBE

### 概要
``` sql
INSERT OVERWRITE CUBE cube_name [WHERE condition]
```

### 描述
类似于INSERT INTO CUBE语句，但使用此语句覆盖现有数据。
谓词是可选的。

### 例子
根据条件插入数据到`orders_cube`Cube：

    INSERT OVERWRITE CUBE orders_cube WHERE orderdate > date '1999-01-01';
    INSERT OVERWRITE CUBE orders_cube;

## SHOW CUBES

### 概要
```sql
SHOW CUBES [ FOR table_name ];
```

### 描述
`SHOW CUBES`列出所有立方体。添加可选的`table_name`仅列出该表的Cubes。

### 例子

显示所有Cubes:
```sql
    SHOW CUBES;
```

显示`orders`表的Cubes:

```sql
    SHOW CUBES FOR orders;
```

## DROP CUBE

### 概要

``` sql
DROP CUBE  [ IF EXISTS ] cube_name
```

### 描述
删除存在的Cube.

如果Cube不存在，可选的`IF EXISTS`子句会抑制报错。

### 例子

删除Cube`orders_cube`:

    DROP CUBE orders_cube

如果存在，则删除Cube`orders_cube`：

    DROP CUBE IF EXISTS orders_cube


