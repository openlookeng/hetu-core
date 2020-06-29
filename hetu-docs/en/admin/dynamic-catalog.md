+++

weight = 8
title = "Dynamic Catalog"
+++

# Dynamic Catalog

This section introduces the dynamic catalog feature of openLooKeng. Normally openLooKeng admins add data source to the engine by putting a catalog profile (e.g. `hive.properties`) in the connector directory (`etc/catalog`). Whenever there is a requirement to add, update or delete a catalog, all the coordinators and workers need to be restarted.

In order to dynamically change the catalogs on-the-fly, openLooKeng introduced dynamic catalog feature. To enable this feature, configure it in the `etc/config.properties`:

    catalog.dynamic-enabled=true

Then configure the filesystems used to store dynamic catalog information in `hdfs-config-catalog.properties`
and `local-config-catalog.properties`. Check the filesystem doc for more information.

## Usage

The catalog operations are done through a RESTful API on the openLooKeng coordinator. A http request has the following shape (hive connector as an example):

    request: POST/DELETE/PUT
    
    header: ``X-Presto-User: admin``
    
    form: 'catalogInformation={
            "catalogName" : "hive",
            "connectorName" : "hive-hadoop2",
            "properties" : {
                  "hive.hdfs.impersonation.enabled" : "false",
                  "hive.hdfs.authentication.type" : "KERBEROS",
                  "hive.collect-column-statistics-on-write" : "true",
                  "hive.metastore.service.principal" : "hive/hadoop.hadoop.com@HADOOP.COM",
                  "hive.metastore.authentication.type" : "KERBEROS",
                  "hive.metastore.uri" : "thrift://xx.xx.xx.xx:21088",
                  "hive.allow-drop-table" : "true",
                  "hive.config.resources" : "core-site.xml,hdfs-site.xml",
                  "hive.hdfs.presto.keytab" : "user.keytab",
                  "hive.metastore.krb5.conf.path" : "krb5.conf",
                  "hive.metastore.client.keytab" : "user.keytab",
                  "hive.metastore.client.principal" : "test@HADOOP.COM",
                  "hive.hdfs.wire-encryption.enabled" : "true",
                  "hive.hdfs.presto.principal" : "test@HADOOP.COM"
                  }
              }',
              'catalogConfigurationFiles=path/to/core-site.xml',
              'catalogConfigurationFiles=path/to/hdfs-site.xml',
              'catalogConfigurationFiles=path/to/user.keytab',
              'globalConfigurationFiles=path/to/krb5.conf'

### Add Catalog

When a new catalog is added, a POST request is sent to a coordinator. The coordinator first rewrite the file path properties, saving the files to local disk and verify the operation by loading the newly added catalog. If the catalog is successfully loaded, the coordinator saves the files to the shared file system (e.g. HDFS).

Other coordinators and workers periodically check the catalog properties file in the shared filesystem. When a new catalog is discovered, they pull the related config files to the local disk and then load the catalog into the memory.

### Delete catalog

Similar to adding operation, when a catalog needs to be deleted, send a DELETE request to a coordinator. The coordinator that received the request deletes the related catalog profiles from the local disk, unloads the catalog from server and deletes it from the shared file system.

Other coordinators and workers periodically check the catalog properties file in the shared filesystem. When a catalog is deleted, they also delete the related config files from the local disk and then unload the catalog from the memory.

### Update catalog

An UPDATE operation is a combination of DELETE and ADD operations. First the admin sends a PUT request to a coordinator. On receipt the coordinator deletes and add the catalog locally to verify the change. If this operation is successful, this coordinator delete the catalog from the shared file system and WAIT UNTIL all other nodes to delete the catalog from their local filesystem. After it saves the new configuration files to the shared file system.

Other coordinators and workers periodically check the catalog properties file in the shared filesystem and perform changes accordingly on the local file system.

Catalog properties including ``connector-name`` and ``properties`` can be modified. However, the **catalog name** CAN NOT be changed.

## API information

### HTTP request

Add: `POST host/v1/catalog`

Update: `PUT host/v1/catalog`

Delete: `DELETE host/v1/catalog/{catalogName}`

### HTTP Return code

| HTTP Return code          | POST                                       | PUT                                                       | DELETE       |
|---------------------------|--------------------------------------------|-----------------------------------------------------------|--------------|
| 401 UNAUTHORIZED          | No permission to add a catalog             | No permission to change a catalog                         | Same as PUT  |
| 302 FOUND                 | The catalog already exists                 | -                                                         | -            |
| 404 NOT_FOUND             | Dynamic catalog is disabled                | The catalog does not exist or dynamic catalog is disabled | Same as PUT  |
| 400 BAD_REQUEST           | The request is not correct                 | Same as POST                                              | Same as PUT  |
| 409 CONFLICT              | Another session is operating the catalog   | Same as POST                                              | Same as POST |
| 500 INTERNAL_SERVER_ERROR | Internal error occurred in the coordinator | Same as POST                                              | Same as POST |
| 201 CREATED               | SUCCESS                                    | SUCCESS                                                   | -            |
| 204 NO_CONTENT            | -                                          | -                                                         | SUCCESS      |

## Configuration properties

In `etc/config.properties`:

| Property Name              | Mandatory | Description                                               | Default Value |
|----------------------------|-----------|-----------------------------------------------------------|---------------|
| `catalog.dynamic-enabled`  | NO        | Whether to enable dynamic catalog                         | false         |
| `catalog.scanner-interval` | NO        | Interval for scanning catalogs in the shared file system. | 5s            |
| `catalog.max-file-size`    | NO        | Maximum catalog file size                                 | 128k          |

In `etc/node.properties`:

| Property Name              | Mandatory | Description                                                               | Default Value |
|----------------------------|-----------|---------------------------------------------------------------------------|---------------|
| `catalog.config-dir`       | YES       | Root directory for storing configuration files in local disk.             |               |
| `catalog.share.config-dir` | YES       | Root directory for storing configuration files in the shared file system. |               |

## Impact on queries

- After a catalog is added, queries may fail during the scanning period.
- After a catalog is deleted, queries that are being executed may fail. They may be able to finish during the scanning period.
- Queries in progress may fail when the catalog is being updated. After the catalog is updated, queries may fail during the scanning period.

