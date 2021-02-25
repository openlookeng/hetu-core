# Dynamic Filtering
This section describes the openLooKeng dynamic filtering features. Dynamic filtering is suitable for highly selective join scenarios, i.e., most of the tables on the probe side are filtered out after reading because they do not match the join conditions.

openLooKeng generates dynamic filter conditions based on join conditions and data read from build side during query run, and is applied to the table scan stage of probe side table as an additional filter condition, to reduce the data volume of probe table participating in join operation and effectively reduce IO read and network transmission.

## scenarios
The dynamic filtering is primarily used to optimize the highly selective join scenarios (including dynamic partition pruning for large partitioned tables and row filtering for non-partitioned tables). openLooKeng dynamic filtering is currently applicable to `inner join`, `semi join` and `right join` scenarios, only can be applied to `Hive connector`, `DC connector` and `Memory connector`.

## Usage
openLooKeng dynamic filtering feature depends on the distributed cache component. Please refer to the section [State Store](state-store.md) for specific configuration on state store.
In `/etc/config.properties`, the following parameters need to be configured.

``` properties
enable-dynamic-filtering=true
dynamic-filtering-data-type=BLOOM_FILTER
dynamic-filtering-max-per-driver-size=100MB
dynamic-filtering-max-per-driver-row-count=10000
dynamic-filtering-bloom-filter-fpp=0.1
```

The above attributes are described below:

- `enable-dynamic-filtering`: Enable dynamic filtering feature.
- `dynamic-filtering-wait-time`: Maximum waiting time for the dynamic filter to be ready, default to 1s. 
- `dynamic-filtering-data-type`: Set dynamic filtering data type, default to BLOOM_FILTER.
- `dynamic-filtering-max-size`: Max dynamic filter size, cost based optimizer won't create dynamic filter that has estimate size exceeding this value based on statistics, default to 1000000.
- `dynamic-filtering-max-per-driver-size`: Max data size collected for dynamic filter per driver, default to 1MB.
- `dynamic-filtering-max-per-driver-row-count`: Max data count collected for dynamic filter per driver, default to 10000.
- `dynamic-filtering-bloom-filter-fpp`: Bloom filter FPP used for dynamic filtering, default to 0.1.

If applied to `Hive connector`: we should change `catalog/hive.properties`:
``` properties
hive.dynamic-filter-partition-filtering=true
hive.dynamic-filtering-row-filtering-threshold=5000
```

The above attributes are described below:
- `hive.dynamic-filter-partition-filtering`: Filter out hive splits early based on partition value using dynamic filter, default to false.
- `hive.dynamic-filtering-row-filtering-threshold`: Filter out hive rows early if the dynamic filter size is below the threshold, default to 2000.

## Query Plans
The following example shows a query using the dynamic filter, labeled as **dynamicFilter**. We can use the explain command to see whether the dynamic filter works and can also get it form the webUI with the live plan.

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
