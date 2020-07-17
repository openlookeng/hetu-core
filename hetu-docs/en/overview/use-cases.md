
# Use Cases

## Cross-Source Heterogeneous Query Scenario

Data management systems like RDBMS (such as MySQL and Oracle) and NoSQL (such as HBase, ES, and Kafka) are widely used in various application systems of customers. With the increase of data volume and better data management, customers gradually build data warehouses based on Hive or MPPDB. These data storage systems are often isolated from each other, resulting in independent data silos. Data analysts often suffer from the following problems:
  1. Unaware of where and how to use the data you want, you cannot build a new service model based on massive data.
  2. Querying various data sources requires different connection modes or clients and running different SQL dialects. These differences cause extra learning costs and complex application development logic.
  3. If data is not aggregated, federated query cannot be performed on data from different systems.

openLooKeng can be used to implement joint query of data warehouses such as RDBMS, NoSQL, Hive, and MPPDB. With the cross-source heterogeneous query capability of openLooKeng, data analysts can quickly analyze massive data.

## Cross-domain and cross-DC query

In a two-level or multi-level data center scenario, for example, a province-city data center or a headquarters-branch data center, users often need to query data from the provincial (headquarters) data center or municipal (branch) data center, the bottleneck of cross-domain query is the network problems (such as insufficient bandwidth, long delay, and packet loss) between multiple data centers. As a result, the query delay is long and the performance is unstable.
openLooKeng is a cross-domain and cross-DC solution designed for cross-domain queries. The openLooKeng cluster is deployed in multiple DCs. After the openLooKeng cluster in DC2 completes computing, the result is transmitted to the openLooKeng cluster in DC1 through the network, aggregation calculation is completed in the openLooKeng cluster of DC1.
In the cross-domain and cross-DC openLooKeng solution, calculated results are transmitted between openLooKeng clusters. This avoids network problems caused by insufficient network bandwidth and packet loss, and solves the cross-domain query problem to some extent.

## Separated Storage & Compute

openLooKeng itself does not have a storage engine, but it can query data stored in different data sources. Therefore, the system is a typical storage and computing separated system, which aids to expand the compute and storage systems independently.
The openLooKeng storage and computing separation architecture is suitable for dynamic expansion clusters, facilitating quick elastic scaling of resources.

## Quick Data Exploration

Customers have a large amount of data. To use the data, they usually build a dedicated data warehouse. However, this will cause extra labor costs for data warehouse maintenance and data ETL time costs. For customers who need to quickly explore data but do not want to build a dedicated data warehouse, it is time-consuming and labor-intensive to replicate data and load the data to the data warehouse.
openLooKeng can use standard SQL to define a virtual data mart and connect to each data source with the cross-source heterogeneous query capability. In this way, various analysis tasks that users need to explore can be defined at the semantic layer of the virtual data mart.
With the data virtualization capability of openLooKeng, customers can quickly build exploration and analysis services based on various data sources without building complex and dedicated data warehouses.
