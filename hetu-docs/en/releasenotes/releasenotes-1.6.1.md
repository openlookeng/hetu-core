# Release 1.6.1 (27 Apr 2022)

## Key Features

This release is mainly about modification and enhancement of some SPIs, which are used in more scenarios.

| Area                    | Feature                                                      | PR #s                                                        |
| ----------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Data Source statistics                | The method of obtaining statistics is added so that statistics can be directly obtained from the Connector. Some operators can be pushed down to the Connector for calculation. You may need to obtain statistics from the Connector to display the amount of processed data.                                               | 1450                                                          |
| Operator processing extension | Users can customize the physical execution plan of worker nodes. Users can use their own operator pipelines to replace the native implementation to accelerate operator processing. | 1436                                                           |
| HIVE UDF extension | Adds the adaptation of HIVE UDF function namespace to support the execution of UDFs (including GenericUDF) written based on the HIVE UDF framework. | 1456                                                           |

## Obtaining the Document 

For details, see [https://gitee.com/openlookeng/hetu-core/tree/1.6.1/hetu-docs/en](https://gitee.com/openlookeng/hetu-core/tree/1.6.1/hetu-docs/en)