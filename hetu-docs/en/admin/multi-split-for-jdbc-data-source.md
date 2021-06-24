# JDBC Data Source Multi-Split Management

## Overview

This function applies to JDBC data sources. Data tables to be read are divided into multiple splits, and multiple worker nodes in the cluster simultaneously read the splits to accelerate data reading.

## Properties

Multi-split management is based on connectors. For a data table with this function enabled, add the following attributes to the configuration file of the connector to which the data table belong. For example, the configuration file corresponding to the **mysql** connector is **etc/mysql.properties**.

Property list:

Configure the properties as follows:

```properties
jdbc.table-split-enabled=true
jdbc.table-split-stepCalc-refresh-interval=10s
jdbc.table-split-stepCalc-threads=2
jdbc.table-split-fields=[{"catalogName":"test_catalog", "schemaName":null, "tableName":"test_table", "splitField":"id","dataReadOnly":"true", "calcStepEnable":"false", "splitCount":"5","fieldMinValue":"1","fieldMaxValue":"10000"},{"catalogName":"test_catalog1", "schemaName":"test_schema1", "tableName":"test_tabl1", "splitField":"id", "dataReadOnly":"false", "calcStepEnable":"true", "splitCount":"5", "fieldMinValue":"","fieldMaxValue":""}]
```

Descriptions of the properties:

- `jdbc.table-split-enabled`: whether to enable the multi-split data read function. The default value is **false**.
- `jdbc.table-split-stepCalc-refresh-interval`: interval for dynamically updating splits. The default value is 5 minutes.
- `jdbc.table-split-stepCalc-threads`: number of threads for dynamically updating splits. The default value is **4**.
- `jdbc.table-split-fields`: split configuration of each data table. For details, see section "Split Configuration".

### Split Configuration

The configuration of each data table consists of multiple sub-properties, which are set in the JSON format. The description is as follows:

> | Sub-property| Description| Suggestion|
> |----------|----------|----------|
> | `catalogName`| Name of the catalog to which the data table belongs in the data source, which corresponds to the value of the **TABLE\_CAT** field returned by the standard JDBC API **DatabaseMetaData.getTables**.| Set this sub-property to the actual value. If the value is empty, set it to **null**. |
> | `schemaName`| Name of the schema to which the data table belongs in the data source, which corresponds to the value of the **TABLE\_SCHEM** field returned by the standard JDBC API **DatabaseMetaData.getTables**.| Set this sub-property to the actual value. If the value is empty, set it to **null**. |
> | `tableName`| Name of the data table in the data source, which corresponds to the value of the **TABLE\_NAME** field returned by the standard JDBC API **DatabaseMetaData.getTables**.| Set this sub-property based on the actual value. |
> | `splitField`| Column name of the split | Select a column whose value is an integer. You are advised to select a column with fewer duplicate values to divide the column into even splits.|
> | `calcStepEnable`| Whether to dynamically adjust the split range| Set this sub-property to **true** for data tables with data changes. |
> | `dataReadOnly`| Whether the data table is read-only| Set this sub-property to **true** for read-only data tables. |
> | `splitCount`| Number of concurrent reads of data splits| Set this sub-property based on the optimal value. |
> | `fieldMinValue`| Minimum value of the **splitField** field| Set this sub-property for read-only data tables based on the query result. Otherwise, leave this sub-property empty or set it to **null**. |
> | `fieldMaxValue`| Maximum value of the **splitField** field| Set this sub-property for read-only data tables based on the query result. Otherwise, leave this sub-property empty or set it to **null**. |

