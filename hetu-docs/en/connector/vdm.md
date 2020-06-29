# VDM Connector

There are use cases that multiple data sources need to be managed and visited together in one single session or view. Also, users may not care about the distribution and source of data at all. The VDM  (Virtualize Data Market) connector is aimed at bringing in this feature to openLooKeng.

The VDM connector supports to:

- Create, update and delete views that combines multiple catalogs
- Visit real data through the views
- Manage user previlege through the views
- Log the use of VDM views by each user

## <a name="usage_section">Usage</a>

VDM uses openLooKeng metastore to store its database information. It can be stored either on HDFS or relational database, depending on the implementation of openLooKeng metastore. 

Therefore metastore must be configured first. Here is a example of using RDBMS as metastore, create `etc/hetu-metastore.properties`:

    hetu.metastore.type=jdbc
    hetu.meatstore.db.url=jdbc:mysql://....
    hetu.metastore.db.user=root
    hetu.metastore.db.password=123456

For user interface, the connector can be accessed from JDBC or command line interface. Currently VDM only supports schemas and views. Tables are NOT supported.

Schema operations are the same as usual openLooKeng catalogs, including `create schema`, `drop schema`, `rename schema` and `show schemas`. 

Views can be created under a specific schema: `create view as ...`, `drop view`.

## Example usage:

Configure a data source `vdm1` by creating `vdm1.properties` in `etc/catalogs` with following contents:

    connector.name=vdm

This example creates a schema `schema1` in `vdm1` catalog, and creates two views from two other different data sources. Note that metastore must be configured in advance (See [usage](#usage_section) section).

    create schema vdm1.schema1;
    use vdm1.schema1;
    create view view1 as select * from mysql.table.test;
    create view view2 as select * from hive.table.test;
    select * from view1;

VDM datasource can also be managed through dynamic catalog API. See [Dynamic Catalog](../admin/dynamic-catalog.md) topic for more information.

## All supported CLI queries

| Support operation               | External interface (SQL command) |
| :------------------------------ | :------------------------------- |
| Add VDM                         | `create catalog`(resulful)       |
| Remove VDM                      | `drop catalog`(resulful)         |
| Query all VDM                   | `show catalogs`                  |
| Create schema                   | `create schema`                  |
| Delete schema                   | `drop schema`                    |
| Rename schema                   | `rename schema`                  |
| Query all schemas under VDM     | `show schemas`                   |
| Query all views in the schema   | `show tables`                    |
| Create/Update View              | `create [or replace] view`       |
| Delete view                     | `drop view`                      |
| Query data by view              | `select * from view`             |
| Query view creation information | `show create view`               |
| Query view column information   | `desc view`                      |

