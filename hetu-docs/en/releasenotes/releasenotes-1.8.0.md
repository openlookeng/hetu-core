# Release 1.8.0

## Key Features

| Area                      | Feature                                                                                                                                                                                                                              |
|---------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Task Level Recovery       | 1. Enables query recovery by retrying only failed task(s) using exchange materialization<br/> 2. Supported using HDFS for spooling<br/> 3. Supported Direct Serialization and compression too while materializing the exchange data. |
| Query Resource Management | Query level resource monitoring that triggers mitigation actions like suspend/resume query or task, spill revocable memory, throttle scheduling, and kill for problematic queries in a resource group.                               |
| Connector Extension       | The singleData connector supplements the OLAP capability for the openGauss, enhances the capability of data analysis. Supports adaptor to the ShardingSphere and tidRange modes.                                                     |

## Obtaining the Document 

For details, see [https://gitee.com/openlookeng/hetu-core/tree/1.8.0/hetu-docs/en](https://gitee.com/openlookeng/hetu-core/tree/1.8.0/hetu-docs/en)

