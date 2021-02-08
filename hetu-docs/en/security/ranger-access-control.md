
Ranger Access Control
==============================

Overview
-------------------------

Apache Ranger delivers a comprehensive approach to security for a Hadoop cluster. It provides a centralized platform to define, administer and manage security policies consistently across Hadoop components. Check [Apache Ranger Wiki](https://cwiki.apache.org/confluence/display/RANGER/Index) for detail introduction and user guide.

[openlookeng-ranger-plugin](https://gitee.com/openlookeng/openlookeng-ranger-plugin) is a ranger plugin for openLooKeng to enable, monitor and manage comprehensive data security.

Build Process
-------------------------

1. Check out [openlookeng-ranger-plugin](https://gitee.com/openlookeng/openlookeng-ranger-plugin) code from GIT repository

2. On the root folder, please execute the following Maven command :

```
mvn clean package
```

3. After the above build command execution, you would see the following TAR files in the target folder :

```
ranger-<ranger.version>-admin-openlookeng-<openlookeng.version>-plugin.tar.gz
ranger-<ranger.version>-openlookeng-<openlookeng.version>-plugin.tar.gz
```

Deployment Process
-------------------------

### Install Ranger Admin plugin

1). Expand the ranger-&lt;ranger.version&gt;-admin-openlookeng-&lt;openlookeng.version&gt;-plugin.tar.gz file, you would see the following folders in the target folder :

```
openlookeng
service-defs
```

2). Register Service Type definition with Ranger

Service type definition should be registered with Ranger using REST API provided by Ranger Admin.  Once registered, Ranger Admin will provide UI to create service instances (called as repositories in previous releases) and policies for the service-type. Ranger plugin uses the service type definition and the policies to determine if an access request should be granted or not. The REST API can be invoked using curl command as shown in the example below :

```
curl -u admin:password -X POST -H "Accept: application/json" -H "Content-Type: application/json" -d @service-defs/ranger-servicedef-openlookeng.json http://ranger-admin-host:port/service/plugins/definitions
```

3). Copy openlookeng folder to ranger-plugins folder of Ranger Admin installed directory (e.g. ranger-&lt;ranger.version&gt;-admin/ews/webapp/WEB-INF/classes/ranger-plugins/)

### Install openLooKeng plugin

1). Expand the ranger-&lt;ranger.version&gt;-openlookeng-&lt;openlookeng.version&gt;-plugin.tar.gz file

2). Modify the install.properties file with appropriate variables. There is an example that some variables were modified as follows :

> ```properties
> # Location of Policy Manager URL
> # Example: POLICY_MGR_URL=http://policymanager.xasecure.net:6080
> POLICY_MGR_URL=http://xxx.xxx.xxx.xxx:6080
> 
> # This is the repository name created within policy manager
> # Example: REPOSITORY_NAME=openlookengdev
> REPOSITORY_NAME=openlookengdev
>
> # openLooKeng component installed directory
> # COMPONENT_INSTALL_DIR_NAME=../openlookeng
> COMPONENT_INSTALL_DIR_NAME=/home/hetu-server-1.0.1
>
> XAAUDIT.SOLR.ENABLE=false
> XAAUDIT.SUMMARY.ENABLE=false
> ```

3). Execute ./enable-openlookeng-plugin.sh

### Restart service

```
Restart Ranger Admin service: service ranger-admin restart
Restart openLooKeng service: ./launcher restart
```

Ranger Service Manager
-------------------------

You can click on the plus icon next to `OPENLOOKENG` column on the Ranger Admin's Service Manager page to add openLooKeng service. The service config properties are listed below :

| Label                 | Description                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| Service Name | Name of the service, you will need to specify the service name in the install.properties file of openLooKeng plugin |
| Display Name | Display Name of the service |
| Description | Give service description for reference |
| Active Status| You can choose this option to enable or disable the service |
| Username | Specify the end system user name that can be used for connection |
| Password | Add the password for username above |
| jdbc.driverClassName | Specify the full classname of the driver used for openLooKeng connections. The default classname is : io.hetu.core.jdbc.OpenLooKengDriver |
| jdbc.url | jdbc:lk://host:port/catalog |
| Add new configurations | Specify any other new configurations |

Ranger Policy Manager
-------------------------

You can add a new policy from the Ranger Admin's Policy Listing Page of openLooKeng service. The ranger plugin of openLooKeng supports manager the privileges of systemproperty, catalog, sessionproperty, schema, table and column.

###  Privilege Statement

- systemproperty
    - alter : Set a system session property value.
- catalog
    - use : If not grant `use` privilege to catalog, all operations under the catalog have no permission.
    - select : Only the `select` privilege was granted, the catalog can be available for show catalogs.
    - create : Create a new, empty schema.
    - show : List the available schemas in `catalog` or in the current catalog.
- sessionproperty
    - alter : Set a catalog session property value.
- schema
    - drop : Drop an existing schema.
    - alter : Change the definition of an existing schema.
    - select : Only the `select` privilege was granted, the schema can be available for show schemas.
    - create : Create a new, empty table with the specified columns; Create a new view of a select query.
    - show : List the available tables in `schema` or in the current schema.
- table
    - drop : Drop existing table, view and column.
    - alter : Change the definition of an existing table; Set the comment for a table; Update the values of the specified columns in all rows that satisfy the condition.
    - insert : Insert new rows into a table.
    - delete : Delete rows from a table.
    - grant : Grants the specified privileges to the specified grantee.
    - revoke : Revokes the specified privileges from the specified grantee.
    - select : Only the `select` privilege was granted, the table can be available for show tables.
    - show : List the available columns in `table` along with their data type and other attributes.
- column
    - select : Retrieve rows from zero or more tables; Only the `select` privilege was granted, the column can be available for show columns.

###  Policy Reference

- Basically, `use` and `select` privileges should be granted to a particular catalog. `show` or `create` privileges are optional to enable show or create schemas.

| Label                 | Description                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| Policy Name | Enter an appropriate policy name. |
| catalog | Select the appropriate catalog. |
| none | Label none indicates don't need to config other resources. |

- If you want to access metadata of the catalog (e.g. show schemas/tables/columns), you should grant `select` privilege to `information_schema` of particular catalog.

| Label                 | Description                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| Policy Name | Enter an appropriate policy name. |
| catalog | Select the appropriate catalog. |
| schema | select `information_schema`. |
| table  | * (indicates select all tables) |
| column | * (indicates select all columns) |

- You can create a policy for system session property.

| Label                 | Description                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| Policy Name | Enter an appropriate policy name. |
| systemproperty | Specify the appropriate system session property. |

- You can create a policy for catalog session property.

| Label                 | Description                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| Policy Name | Enter an appropriate policy name. |
| catalog | Select the appropriate catalog. |
| sessionproperty | Specify the appropriate catalog session property. |

- You can create a policy for a combination for catalog, schema, table and column.

| Label                 | Description                                                  |
| :-------------------------- | :----------------------------------------------------------- |
| Policy Name | Enter an appropriate policy name. |
| catalog | Select the appropriate catalog. |
| schema | For the selected catalog(s), select schema(s) for the which the policy will be applicable. |
| table  | For the selected catalog(s) and schema(s), select table(s) for the which the policy will be applicable.  |
| column | For the selected catalog(s), schema(s) and table(s), select column(s) for the which the policy will be applicable |
