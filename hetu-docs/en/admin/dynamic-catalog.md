
# Dynamic Catalog

This section introduces the dynamic catalog feature of openLooKeng. Normally openLooKeng admins add data source to the engine by putting a catalog profile (e.g. `hive.properties`) in the connector directory (`etc/catalog`). Whenever there is a requirement to add, update or delete a catalog, all the coordinators and workers need to be restarted.

In order to dynamically change the catalogs on-the-fly, openLooKeng introduced dynamic catalog feature.The principle of a dynamic catalog is to manage the catalog-related configuration files on a shared file system and then synchronize all coordinator and worker nodes from the shared file system to local and load them. To enable this feature.

* First, configure it in the `etc/config.properties`:
    ```
    catalog.dynamic-enabled=true
    ```
* Secondly, configure the filesystems used to store dynamic catalog information in `hdfs-config-default.properties`.
You can change this name of file by `catalog.share.filesystem.profile` property in `etc/node.properties`, default value is `hdfs-config-default`.
Check the [filesystem doc](../develop/filesystem.md) for more information.

    Add a `hdfs-config-default.properties` file in the `etc/filesystem/` directory, if this directory does not exist, please create it.
    ```
    fs.client.type=hdfs
    hdfs.config.resources=/opt/openlookeng/config/core-site.xml, /opt/openlookeng/config/hdfs-site.xml
    hdfs.authentication.type=NONE
    fs.hdfs.impl.disable.cache=true
    ```
    If HDFS enable the Kerberos, then
    ```
    fs.client.type=hdfs
    hdfs.config.resources=/opt/openlookeng/config/core-site.xml, /opt/openlookeng/config/hdfs-site.xml
    hdfs.authentication.type=KERBEROS
    hdfs.krb5.conf.path=/opt/openlookeng/config/krb5.conf
    hdfs.krb5.keytab.path=/opt/openlookeng/config/user.keytab
    hdfs.krb5.principal=openlookeng@HADOOP.COM # replace openlookeng@HADOOP.COM to your principal 
    fs.hdfs.impl.disable.cache=true
    ```
 * Finally, configure the paths of filesystems in `etc/node.properties`. Used to specify the path of the configuration file associated with the local storage directory on the shared file system, and because the configuration file needs to be synchronized from the same path on the shared file system, the shared file system path of all coordinator nodes and worker nodes must be consistent, local storage path isn't required.
    ```
    catalog.config-dir=/opt/openlookeng/catalog
    catalog.share.config-dir=/opt/openkeng/catalog/share
    ```

## Usage

The catalog operations are done through a RESTful API on the openLooKeng coordinator. A http request has the following shape (hive connector as an example), the form of POST/PUT body is `multipart/form-data`:

    curl --location --request POST 'http://your_coordinator_ip:9101/v1/catalog' \
    --header 'X-Presto-User: admin' \
    --form 'catalogInformation="{
            \"catalogName\" : \"hive\",
            \"connectorName\" : \"hive-hadoop2\",
            \"properties\" : {
                  \"hive.hdfs.impersonation.enabled\" : \"false\",
                  \"hive.hdfs.authentication.type\" : \"KERBEROS\",
                  \"hive.collect-column-statistics-on-write\" : \"true\",
                  \"hive.metastore.service.principal\" : \"hive/hadoop.hadoop.com@HADOOP.COM\",
                  \"hive.metastore.authentication.type\" : \"KERBEROS\",
                  \"hive.metastore.uri\" : \"thrift://xx.xx.xx.xx:21088\",
                  \"hive.allow-drop-table\" : \"true\",
                  \"hive.config.resources\" : \"core-site.xml,hdfs-site.xml\",
                  \"hive.hdfs.presto.keytab\" : \"user.keytab\",
                  \"hive.metastore.krb5.conf.path\" : \"krb5.conf\",
                  \"hive.metastore.client.keytab\" : \"user.keytab\",
                  \"hive.metastore.client.principal\" : \"test@HADOOP.COM\",
                  \"hive.hdfs.wire-encryption.enabled\" : \"true\",
                  \"hive.hdfs.presto.principal\" : \"test@HADOOP.COM\"
                  }
              }
    "' \
    --form 'catalogConfigurationFiles=@"/path/to/core-site.xml"' \
    --form 'catalogConfigurationFiles=@"/path/to/hdfs-site.xml"' \
    --form 'catalogConfigurationFiles=@"/path/to/user.keytab"', \
    --form 'globalConfigurationFiles=@"/path/to/krb5.conf"'

`catalogName` catalog name specified for the user;

`connectorName` is a catalog type, please refer to connector section;

`properties` for parameters that specify the type of catalog, refer to the detailed configuration of each connector;

If you need to specify a file in the configuration, you only need to specify the file name in the `properties`, and the local path of the file is set as below::

1. Put the file path in a `globalConfigurationFiles` parameter, such as `krb5.conf`, if the configuration file is shared by all catalogs;

2. Put the file path in a `catalogConfigurationFiles` parameter if the configuration file is used only by the currently created catalog, for example `hdfs-site.xml`,`core-site.xml`,`user.keytab`,each catalog has a different configuration.

### Add Catalog

When a new catalog is added, a POST request is sent to a coordinator. The coordinator first rewrite the file path properties, saving the files to local disk and verify the operation by loading the newly added catalog. If the catalog is successfully loaded, the coordinator saves the files to the shared file system (e.g. HDFS).

Other coordinators and workers periodically check the catalog properties file in the shared filesystem. When a new catalog is discovered, they pull the related config files to the local disk and then load the catalog into the memory.

Take Hive as an example, by curl you can create a catalog with the following command:

```shell
curl --location --request POST 'http://your_coordinator_ip:8090/v1/catalog' \
--header 'X-Presto-User: admin' \
--form 'catalogInformation="{
        \"catalogName\" : \"hive\",
        \"connectorName\" : \"hive-hadoop2\",
        \"properties\" : {
              \"hive.hdfs.impersonation.enabled\" : \"false\",
              \"hive.hdfs.authentication.type\" : \"KERBEROS\",
              \"hive.collect-column-statistics-on-write\" : \"true\",
              \"hive.metastore.service.principal\" : \"hive/hadoop.hadoop.com@HADOOP.COM\",
              \"hive.metastore.authentication.type\" : \"KERBEROS\",
              \"hive.metastore.uri\" : \"thrift://xx.xx.xx.xx:21088\",
              \"hive.allow-drop-table\" : \"true\",
              \"hive.config.resources\" : \"core-site.xml,hdfs-site.xml\",
              \"hive.hdfs.presto.keytab\" : \"user.keytab\",
              \"hive.metastore.krb5.conf.path\" : \"krb5.conf\",
              \"hive.metastore.client.keytab\" : \"user.keytab\",
              \"hive.metastore.client.principal\" : \"test@HADOOP.COM\",
              \"hive.hdfs.wire-encryption.enabled\" : \"true\",
              \"hive.hdfs.presto.principal\" : \"test@HADOOP.COM\"
              }
          }
"' \
--form 'catalogConfigurationFiles=@"/path/to/core-site.xml"' \
--form 'catalogConfigurationFiles=@"/path/to/hdfs-site.xml"' \
--form 'catalogConfigurationFiles=@"/path/to/user.keytab"', \
--form 'globalConfigurationFiles=@"/path/to/krb5.conf"'
```

### Delete catalog

Similar to adding operation, when a catalog needs to be deleted, send a DELETE request to a coordinator. The coordinator that received the request deletes the related catalog profiles from the local disk, unloads the catalog from server and deletes it from the shared file system.

Other coordinators and workers periodically check the catalog properties file in the shared filesystem. When a catalog is deleted, they also delete the related config files from the local disk and then unload the catalog from the memory.

Take Hive as an example, by curl you can delete the catalog with the following command, after `catalog` specify the catalog name created earlier:

```shell
curl --location --request DELETE 'http://your_coordinator_ip:8090/v1/catalog/hive' \
--header 'X-Presto-User: admin'
```

### Update catalog

An UPDATE operation is a combination of DELETE and ADD operations. First the admin sends a PUT request to a coordinator. On receipt the coordinator deletes and add the catalog locally to verify the change. If this operation is successful, this coordinator delete the catalog from the shared file system and WAIT UNTIL all other nodes to delete the catalog from their local filesystem. After it saves the new configuration files to the shared file system.

Other coordinators and workers periodically check the catalog properties file in the shared filesystem and perform changes accordingly on the local file system.

Catalog properties including ``connector-name`` and ``properties`` can be modified. However, the **catalog name** CAN NOT be changed.

Take Hive as an example, by curl you can update the catalog with the following command, the following updated `hive.allow-drop-table` parameters:

```shell
curl --location --request PUT 'http://your_coordinator_ip:8090/v1/catalog' \
--header 'X-Presto-User: admin' \
--form 'catalogInformation="{
        \"catalogName\" : \"hive\",
        \"connectorName\" : \"hive-hadoop2\",
        \"properties\" : {
              \"hive.hdfs.impersonation.enabled\" : \"false\",
              \"hive.hdfs.authentication.type\" : \"KERBEROS\",
              \"hive.collect-column-statistics-on-write\" : \"true\",
              \"hive.metastore.service.principal\" : \"hive/hadoop.hadoop.com@HADOOP.COM\",
              \"hive.metastore.authentication.type\" : \"KERBEROS\",
              \"hive.metastore.uri\" : \"thrift://xx.xx.xx.xx:21088\",
              \"hive.allow-drop-table\" : \"false\",
              \"hive.config.resources\" : \"core-site.xml,hdfs-site.xml\",
              \"hive.hdfs.presto.keytab\" : \"user.keytab\",
              \"hive.metastore.krb5.conf.path\" : \"krb5.conf\",
              \"hive.metastore.client.keytab\" : \"user.keytab\",
              \"hive.metastore.client.principal\" : \"test@HADOOP.COM\",
              \"hive.hdfs.wire-encryption.enabled\" : \"true\",
              \"hive.hdfs.presto.principal\" : \"test@HADOOP.COM\"
              }
          }
"' \
--form 'catalogConfigurationFiles=@"/path/to/core-site.xml"' \
--form 'catalogConfigurationFiles=@"/path/to/hdfs-site.xml"' \
--form 'catalogConfigurationFiles=@"/path/to/user.keytab"', \
--form 'globalConfigurationFiles=@"/path/to/krb5.conf"'
```



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
| `catalog.dynamic-enabled`  | NO        | Whether to enable dynamic catalog.                        | false         |
| `catalog.scanner-interval` | NO        | Interval for scanning catalogs in the shared file system. | 5s            |
| `catalog.max-file-size`    | NO        | Maximum catalog file size, in order to avoid excessive file size being uploaded by client. | 128k          |
| `catalog.valid-file-suffixes`    | NO        |The valid suffixes for catalog configuration file, if there are several suffixes, separate them by commas.empty suffix means allow all files. This configuration is used to control the types of files being uploaded.|           |

In `etc/node.properties`:

Path white list：["/tmp", "/opt/hetu", "/opt/openlookeng", "/etc/hetu", "/etc/openlookeng", current workspace]

Notice：avoid to choose root directory; ../ can't include in path; if you config node.date_dir, then the current workspace is the parent of node.data_dir;
otherwise, the current workspace is the openlookeng server's directory.

| Property Name              | Mandatory | Description                                                               | Default Value |
|----------------------------|-----------|---------------------------------------------------------------------------|---------------|
| `catalog.config-dir`       | YES       | Root directory for storing configuration files in local disk.             |               |
| `catalog.share.config-dir` | YES    | Root directory for storing configuration files in the shared file system. |               |
| `catalog.share.filesystem.profile` | NO       | The profile name of the shared file system. | hdfs-config-default |

## Impact on queries

- After a catalog is deleted, queries that are being executed may fail. 
- Queries in progress may fail when the catalog is being updated. 

