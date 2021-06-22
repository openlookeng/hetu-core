# JDBC数据源多分片管理

## 功能介绍
该功能适用于JDBC数据源，旨在通过把待读取的数据表划分为多个分片，并由集群的多个worker节点同时发起读取数据分片，以达到加快数据读取速度的效果。

## 属性配置

多分片管理的配置以连接器为单位，开启该功能的数据表需要在所属的连接器的配置文件（例如名为mysql的连接器对应的配置文件为etc/mysql.properties）添加以下属性：

属性列表：
配置文件配置项说明:

``` properties
jdbc.table-split-enabled=true
jdbc.table-split-stepCalc-refresh-interval=10s
jdbc.table-split-stepCalc-threads=2
jdbc.table-split-fields=[{"catalogName":"test_catalog", "schemaName":null, "tableName":"test_table", "splitField":"id","dataReadOnly":"true", "calcStepEnable":"false", "splitCount":"5","fieldMinValue":"1","fieldMaxValue":"10000"},{"catalogName":"test_catalog1", "schemaName":"test_schema1", "tableName":"test_tabl1", "splitField":"id", "dataReadOnly":"false", "calcStepEnable":"true", "splitCount":"5", "fieldMinValue":"","fieldMaxValue":""}]
```

上述属性说明如下：

   - `jdbc.table-split-enabled`：是否开启多分片读取数据功能，默认为false。
   - `jdbc.table-split-stepCalc-refresh-interval`：动态更新分片的取值范围的周期，默认值为5分钟。
   - `jdbc.table-split-stepCalc-threads`：动态更新分片的取值范围的线程数，默认值为4。
   - `jdbc.table-split-fields`：各个数据表的分片配置信息，请参考"分片配置信息填写说明"。
    
### 分片配置信息填写说明

每个数据表的配置信息由多个子属性构成，按JSON格式的来填写，说明如下：

> | 子属性名称| 描述| 填写建议|
> |----------|----------|----------|
> | `catalogName`| 该数据表在数据源里所属的catalog的名字，对应标准jdbc的DatabaseMetaData.getTables接口返回的"TABLE_CAT"字段的内容| 按实际值填写，空值请填null|
> | `schemaName`| 该数据表在数据源里所属的schema的名字，对应标准jdbc的DatabaseMetaData.getTables接口返回的"TABLE_SCHEM"字段的内容| 按实际值填写，空值请填null|
> | `tableName`| 该数据表在数据源里的名字，对应标准jdbc的DatabaseMetaData.getTables接口返回的"TABLE_NAME"字段的内容| 需要按实际值填写|
> | `splitField`| 划分分片的列名| 需要选取取值为整型的列，建议选取重复值少分布均匀的列，以便划分出均匀的分片|
> | `calcStepEnable`| 是否需要动态调整分片的区间取值范围| 对存在数据变更的数据表设置为"true"|
> | `dataReadOnly`| 是否是只读数据表| 对只读数据表设置为"true"|
> | `splitCount`| 读取数据分片的并发数| 请按调优的最佳取值来填写|
> | `fieldMinValue`| splitField字段的最小值| 对只读数据表建议按查询结果来配置，否则填""或“null”|
> | `fieldMaxValue`| splitField字段的最大值| 对只读数据表建议按查询结果来配置，否则填""或“null”|