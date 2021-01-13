
# Release 0.1.0

## Key Features

| Feature                                      | Description                                                  |
| -------------------------------------------- | ------------------------------------------------------------ |
| Adaptive Dynamic Filter                      | The dynamic feature is enhanced so that in addition to bloom filter, hashsets can be used to store the build side values to filter out the probe side. The filters are stored in a distributed memory store so that they can be reused by subsequent queries without having to be rebuilt. |
| Dynamically Add Catalog                      | Add catalog REST api allows an administrator to add new catalogs of any connector during run time. Catalog property files are written to a shared storage where they are discovered by openLooKeng cluster nodes and are registered and loaded. This does not require to restart server.|
| Cross Region Dynamic Filter                  | In a cross data center scenario when the probe side is in the remote data center and the build side is in the local data center, the adaptive filter will create a bloom filter. The data center connector is enhanced to send the bloom filter over the network to the remote data center, where it can be used to filter out the probe side. This reduces the amount of data that needs to be transferred over the network from the remote data center to the local data center. |
| Horizontal Scaling Support                   | Introduced ISOLATING and ISOLATED node states so that a node can be quiesced during scale-in. ISOLATED nodes do not accept any new tasks. |
| VDM                                          | Virtual Data Mart, allows an administrator to create virtual catalogs, and virtual schemas. Within a virtual schema, the administrator can create views over tables spanning multiple data sources. Virtual data marts simplifies the access to tables across data sources, and across regions. |
| IUD support for ORC                          | Support Insert, Update, Delete on Hive transactional ORC tables. Only Hive 3.x is supported |
| Compaction for ORC                           | Support compaction of hive transactional ORC tables so that the number of files to be read is reduced there by increasing the data fetch per reader and hence helps in improving the query performance and also improves concurrency |
| CarbonData connector with IUD support        | Support Insert, Update, Delete Operations on CarbonData tables |
| Insert Overwrite                             | Support for Insert overwrite syntax.This is easy method for Truncating and loading into the existing table |
| ODBC connector                               | ODBC Driver and gateway for the 3rd party BI tools like PowerBI, Tableau, YongHong Desktop  to connect to openLooKeng |
| Dynamic Hive UDF                             | Dynamically load custom hive UDFs into openLooKeng                  |
| HBase Connector                              | HBase Connector supports read data from HBase data source                                             |
| Enhance Create Table statement               | Allow users to create Hive transactional ORC table; Allow users to specify the external location of managed hive tables when running CREATE TABLE AS command in openLooKeng |
| Metadata Cache                               | A generic metadata cache SPI is introduced to provide a transparent caching layer that can be leveraged by any connector. The metadata cache delegates to the connector specific metadata if the cache does not exist. Currently being used by JDBC connectors, and DC Connector |
| Cross DC Connector                           | A new connector is introduced to support responsive queries across a WAN allowing a client to query a data source that is presenting in another data center. |
| High Availability (Active-Active)            | Supports HA AA mode by storing runtime state information into a distributed cache like Hazelcast. Hazelcast cluster formation is done using a seed file. Discovery Service, OOM, & CPU Usage uses a distributed lock to ensure only one coordinator starts these services. |
| Sql Migration Tool                           | Supplementary tool to assist in migrating the Hive SQL to openLooKeng Compatible SQLs |
| Code Enhancement                             | RawSlice Optimization: This optimization reduces the memory footprint of openLooKeng by reusing the RawSlice object instead of constructing new Slice objects; Implicit Conversion: This feature supports data type implicit conversion. For example: If the query type does not match the table type in the Insert statement, it can convert the query type to the table type implicitly.|

## Obtaining the Document 

For details, see [https://gitee.com/openlookeng/hetu-core/tree/010/hetu-docs/en](https://gitee.com/openlookeng/hetu-core/tree/010/hetu-docs/en)