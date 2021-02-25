# 动态过滤

本节介绍openLooKeng动态过滤特性。动态过滤适用于高选择性join场景，即大多数的probe侧的表在读取之后由于不匹配join条件而被过滤掉。
openLooKeng在查询运行时，依靠join条件以及build侧读出的数据，生成动态过滤条件，并作为额外的过滤条件应用到probe侧表的table scan阶段，从而减少参与join操作的probe表的数据量，有效地减少IO读取与网络传输。

## 适用场景
openLooKeng动态过滤主要应用于高选择性的join场景（包含针对分区表的分区裁剪以及非分区表的行过滤）。openLooKeng动态过滤当前适用于`inner join`，`semi join` 以及`right join`场景，适用于`Hive connector`，`DC connector`以及`Memory connector`。

## 使用
openLooKeng动态过滤特性依赖于分布式缓存组件，请参考[State Store](state-store.md)章节配置。

在`/etc/config.properties`需要配置如下参数

``` properties
enable-dynamic-filtering=true
dynamic-filtering-data-type=BLOOM_FILTER
dynamic-filtering-max-per-driver-size=100MB
dynamic-filtering-max-per-driver-row-count=10000
dynamic-filtering-bloom-filter-fpp=0.1
```

上述属性说明如下：

- `enable-dynamic-filtering`：是否开启动态过滤特性。
- `dynamic-filtering-wait-time`：等待动态过滤条件生成的最长等待时间，默认值是1s。
- `dynamic-filtering-data-type`：设置动态过滤类型，可选包含`BLOOM_FILTER`以及`HASHSET`，默认类型为`BLOOM_FILTER`。
- `dynamic-filtering-max-size`: 每个dynamic filter的大小上限，如果预估大小超过设定值，代价优化器不会生成对应的dynamic filter，默认值是1000000。
- `dynamic-filtering-max-per-driver-size`：每个driver可以收集的数据大小上限，默认值是1MB。
- `dynamic-filtering-max-per-driver-row-count`：每个driver可以收集的数据条目上限，默认值是10000。
- `dynamic-filtering-bloom-filter-fpp`：动态过滤使用的bloomfilter的FPP值，默认是0.1。

如果应用于`Hive connector`，需要对`catalog/hive.properties`如下修改：
``` properties
hive.dynamic-filter-partition-filtering=true
hive.dynamic-filtering-row-filtering-threshold=5000
```
上述属性说明如下：
- `hive.dynamic-filter-partition-filtering`：使用动态过滤条件根据分区值进行预先过滤，默认值是false。
- `hive.dynamic-filtering-row-filtering-threshold`：如果动态过滤条件大小低于阈值，则应用行过滤，默认值是2000。

## 执行计划
下面的例子展示了SQL语句如何应用动态过滤条件，在执行计划中标记为**dynamicFilter**。 可以使用explain命令查看动态过滤是否应用，也可以在webUI中的liveplan查看当前执行是否应用动态过滤。

``` sql
create table table1 (id integer, year varchar);
create table table2 (id integer, total integer);
insert into table1 values (1, '2019'), (2, '2020'), (3, '2021');
insert into table2 values (1, 100), (2, 200);
``` 

Inner join:

``` sql
explain select t1.id, t1.year from table1 t1, table2 t2 where t1.id = t2.id and t2.total = 200;
Query Plan
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Output[id, year]                                                                                                                                                           
│   Layout: [id:integer, year:varchar]                                                                                                                                     
│   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}                                                                                                                
└─ RemoteExchange[GATHER]                                                                                                                                                  
│   Layout: [year:varchar, id:integer]                                                                                                                                  
│   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}                                                                                                             
└─ InnerJoin[("id" = "id_0")][$hashvalue, $hashvalue_9]                                                                                                                 
│   Layout: [id:integer, year:varchar]                                                                                                                               
│   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}                                                                                                          
│   Distribution: PARTITIONED                                                                                                                                        
│   dynamicFilterAssignments = {id_0 -> 238}                                                                                                                         
├─ RemoteExchange[REPARTITION][$hashvalue]                                                                                                                           
│  │   Layout: [id:integer, year:varchar, $hashvalue:bigint]                                                                                                         
│  │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: ?}                                                                                                      
│  └─ ScanFilterProject[table = memory:0, filterPredicate = true, dynamicFilter = {238 -> "id"}]                                                                     
│         Layout: [id:integer, year:varchar, $hashvalue_8:bigint]                                                                                                    
│         Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}    
│         $hashvalue_8 := "combine_hash"(bigint '0', COALESCE("$operator$hash_code"("id"), 0))                                                                       
│         year := 1                                                                                                                                                  
│         id := 0                                                                                                                                                    
└─ LocalExchange[HASH][$hashvalue_9] ("id_0")                                                                                                                        
│   Layout: [id_0:integer, $hashvalue_9:bigint]                                                                                                                   
│   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: ?}                                                                                                      
└─ RemoteExchange[REPARTITION][$hashvalue_10]                                                                                                                     
│   Layout: [id_0:integer, $hashvalue_10:bigint]                                                                                                               
│   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: ?}                                                                                                   
└─ ScanFilterProject[table = memory:5, filterPredicate = ("total" = 200)]                                                                                      
Layout: [id_0:integer, $hashvalue_11:bigint]                                                                                                            
Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}
$hashvalue_11 := "combine_hash"(bigint '0', COALESCE("$operator$hash_code"("id_0"), 0))                                                                 
total := 1                                                                                                                                              
id_0 := 0
```

Semi join:

``` sql
explain select * from table1 where id in (select id from table2);
Query Plan
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Output[id, year]                                                                                                                                                              
│   Layout: [id:integer, year:varchar]                                                                                                                                        
│   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}                                                                                                                   
└─ RemoteExchange[GATHER]                                                                                                                                                     
│   Layout: [id:integer, year:varchar]                                                                                                                                     
│   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}                                                                                                                
└─ FilterProject[filterPredicate = "expr_6"]                                                                                                                               
│   Layout: [id:integer, year:varchar]                                                                                                                                  
│   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}/{rows: ? (?), cpu: ?, memory: ?, network: ?}                                                                
└─ Project[]                                                                                                                                                            
│   Layout: [id:integer, year:varchar, expr_6:boolean]                                                                                                               
│   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}                                                                                                          
└─ SemiJoin[id = id_1][$hashvalue, $hashvalue_16]                                                                                                                    
│   Layout: [id:integer, year:varchar, $hashvalue:bigint, expr_6:boolean]                                                                                         
│   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}                                                                                                       
│   Distribution: PARTITIONED                                                                                                                                     
│   dynamicFilterId: 279                                                                                                                                          
├─ RemoteExchange[REPARTITION][$hashvalue]                                                                                                                        
│  │   Layout: [id:integer, year:varchar, $hashvalue:bigint]                                                                                                      
│  │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: ?}                                                                                                   
│  └─ ScanFilterProject[table = memory:0, filterPredicate = true, dynamicFilter = {279 -> "id"}]                                                                  
│         Layout: [id:integer, year:varchar, $hashvalue_15:bigint]                                                                                                
│         Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}
│         $hashvalue_15 := "combine_hash"(bigint '0', COALESCE("$operator$hash_code"("id"), 0))                                                                   
│         year := 1                                                                                                                                               
│         id := 0                                                                                                                                                 
└─ LocalExchange[SINGLE] ()                                                                                                                                       
│   Layout: [id_1:integer, $hashvalue_16:bigint]                                                                                                               
│   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: ?}                                                                                                   
└─ RemoteExchange[REPARTITION - REPLICATE NULLS AND ANY][$hashvalue_17]                                                                                        
│   Layout: [id_1:integer, $hashvalue_17:bigint]                                                                                                            
│   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: ?}                                                                                                
└─ ScanProject[table = memory:5]                                                                                                                            
Layout: [id_1:integer, $hashvalue_18:bigint]                                                                                                         
Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}                                             
$hashvalue_18 := "combine_hash"(bigint '0', COALESCE("$operator$hash_code"("id_1"), 0))                                                              
id_1 := 0
```
