# Overview 
openLooKeng is a drop in engine which enables in-situ analytics on any data, anywhere, including geographically remote data sources. It provides a global view of all of your data via its SQL 2003 interface. With high availability, auto-scaling, built-in caching and indexing support, openLooKeng is ready for enterprise workload with required reliability. 

The goal of openLooKeng is to support data exploration, ad hoc queries, and batch processing with near real time latency ranging from 100+ms to minutes, without moving your data around. openLooKeng also supports hierarchical deployment enabling geographically remote openLooKeng clusters to participate in the same query. With its cross region query plan optimization capability, queries involving remote data can achieve close to "local" performance.

## Application Scenarios

### Cross-Source Heterogeneous Query Scenario
Data management systems like RDBMS (such as MySQL and Oracle) and NoSQL (such as HBase, ES, and Kafka) are widely used in various application systems of customers. With the increase of data volume and better data management, customers gradually build data warehouses based on Hive or MPPDB. These data storage systems are often isolated from each other, resulting in independent data islands. Data analysts often suffer from the following problems:
  1. Unaware of where and how to use the data you want, you cannot build a new service model based on massive data.
  2. Querying various data sources requires different connection modes or clients and running different SQL dialects. These differences cause extra learning costs and complex application development logic.
  3. If data is not aggregated, federated query cannot be performed on data of different systems.

openLooKeng can be used to implement joint query of data warehouses such as RDBMS, NoSQL, Hive, and MPPDB. With the cross-source heterogeneous query capability of openLooKeng, data analysts can quickly analyze massive data.

### Cross-domain and cross-DC query
In a two-level or multi-level data center scenario, for example, a province-city data center or a headquarters-branch data center, users often need to query data from the provincial (headquarters) data center or municipal (branch) data center, the bottleneck of cross-domain query is the network problems (such as insufficient bandwidth, long delay, and packet loss) between multiple data centers. As a result, the query delay is long and the performance is unstable.
openLooKeng is a cross-domain and cross-DC solution designed for cross-domain queries. The openLooKeng cluster is deployed in multiple DCs. After the openLooKeng cluster in DC2 completes computing, the result is transmitted to the openLooKeng cluster in DC1 through the network, aggregation calculation is completed in the openLooKeng cluster of DC1.
In the cross-domain and cross-DC openLooKeng solution, calculation results are transmitted between openLooKeng clusters. This avoids network problems caused by insufficient network bandwidth and packet loss, and solves the cross-domain query problem to some extent.

### Separated Storage & Compute
openLooKeng itself does not have a storage engine, but it can query data stored in different data sources. Therefore, the system is a typical storage and computing separated system, which can easily expand the computing and storage resources independently.
The openLooKeng storage and computing separation architecture is suitable for dynamic expansion clusters, facilitating quick elastic scaling of resources.

### Quick Data Exploration
Customers have a large amount of data. To use the data, they usually build a dedicated data warehouse. However, this will cause extra labor costs for data warehouse maintenance and data ETL time costs. For customers who need to quickly explore data but do not want to build a dedicated data warehouse, it is time-consuming and labor-intensive to replicate data and load the data to the data warehouse.
openLooKeng can use standard SQL to define a virtual data mart and connect to each data source with the cross-source heterogeneous query capability. In this way, various analysis tasks that users need to explore can be defined at the semantic layer of the virtual data mart.
With the data virtualization capability of openLooKeng, customers can quickly build exploration and analysis services based on various data sources without building complex and dedicated data warehouses.

## Key Features
### Cross Data Center Connector
A new connector is directly connected to another openLooKeng cluster to implement collaboration between multiple DCs. The key technologies are as follows:
  1. Parallel data access: Data sources are concurrently accessed to improve access efficiency. Clients can concurrently obtain data from the server to accelerate data obtaining.
  2. Data compression: Data is compressed using GZIP compression algorithm before being serialized during data transmission, reducing the amount of data transmitted over the network.
  3. Cross-DC dynamic filtering: Filters data to reduce the amount of data to be pulled from the remote end, ensuring network stability and improving query efficiency.
  4. High availability: In openLooKeng, Coordinator AA is supported. Therefore, you can use a proxy (for example, Nginx) to implement load balancing among Coordinators to achieve high availability.If a coordinator is faulty, the availability of the entire cluster is not affected.

### Dynamic Filtering
In the multi-table join scenario with low correlation, most probe side rows are filtered out because they do not match the join conditions after being read. As a result, unnecessary join calculation, I/O read, and network transmission are caused. Dynamic filtering dynamically generates filtering conditions based on join conditions and data read from the build-side table during query running, and applies the filtering conditions to the table scan phase of the probe-side table. This reduces the data volume of the probe table that participates in the join operation, effectively reducing network transmission and improving performance by 30%.

### Index
openLooKeng increases query efficiency by creating indexes on existing data and storing the indexes outside the data source. Bitmap is a bitmap index that records binary information. It is applicable to the AND operation and can be quickly queried using a dictionary. Bloom uses a bit array to represent a set, which can quickly determine whether a value (only the equal sign is supported) exists in a set. Min-max records the maximum and minimum values in a file, which is applicable to statements that are greater than or less than the condition.

### Cache
openLooKeng creates multiple caches such as metadata cache, execution plan cache, and ORC row data cache in order to improve query performance.

#### Metadata Cache
All Connectors that use JDBC connections cache metadata during the first query to improve the query performance. 

#### ORC Row Data Cache
For ORC files, the ORC row data cache provides an efficient method to cache frequently accessed data to improve query latency. The administrator is able to create a cache on specific tables and partitions.

#### Execution Plan Cache
The execution plan is cached after the first query, rather than discarded after each query request, thereby reducing the preprocessing time and resources required for subsequent queries. By caching these plans, time-consuming plan generation steps can be skipped.

## Obtaining the Design Document

Click [here](https://openlookeng.slite.com/p/channel/EDMAZKydV2MsM5trxJPmLv#) to obtain the design document.

## Next Steps
[Developer Guide](hetu-docs/en/develop/_index.md)
