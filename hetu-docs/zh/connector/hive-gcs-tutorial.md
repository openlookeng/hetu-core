
# Hive连接器GCS教程

## 预先步骤

### 保证访问GCS

[Hadoop Cloud Storage连接器](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage)使得访问Cloud Storage数据成为可能。

如果数据是公开的，则无需任何操作。但是在大多数情况下，数据不是公开的，openLooKeng集群需要能够访问这些数据。这通常通过创建具有访问数据权限的服务账号来实现。可以在[GCP中的服务账号页面](https://console.cloud.google.com/projectselector2/iam-admin/serviceaccounts)上进行此操作。创建服务账号后，需要为其创建密钥，并下载JSON格式的密钥。

### Hive连接器配置

另一个要求是已经在openLooKeng中启用并配置Hive连接器。连接器使用Hive元存储进行数据发现，不限于HDFS上的数据。

**配置Hive连接器**

- Hive元存储地址：
  
  > - GCP上的新Hive元存储：
  >   
  >   如果openLooKeng节点是由GCP提供的，那么Hive元存储也应该在GCP上，以将延迟和成本最小化。在GCP上创建新的Hive元存储的最简单方法是创建一个可以从openLooKeng集群访问的小型Cloud DataProc集群（1个主机，0个工作节点）。完成此步骤后，按照现有Hive元存储的步骤进行操作。
  > 
  > - 现有Hive元存储：
  >   
  >   要使用通过openLooKeng集群使用现有Hive元存储，需要将Hive目录属性文件中的`hive.metastore.uri`属性设置为`thrift://${METASTORE_ADDRESS}:${METASTORE_THRIFT_PORT}`。如果元存储使用身份验证，请参考[Hive安全配置](./hive-security.md)。

- GCS访问：
  
  > 下面是可以在Hive目录属性文件中设置的所有GCS配置属性的示例值：
  > 
  > ``` properties
  > # JSON key file used to access Google Cloud Storage
  > hive.gcs.json-key-file-path=/path/to/gcs_keyfile.json
  > 
  > # Use client-provided OAuth token to access Google Cloud Storage
  > hive.gcs.use-access-token=false
  > ```

### Hive元存储配置信息

如果Hive元存储使用StorageBasedAuthorization，那么还需要访问GCS来执行POSIX权限检查。为Hive配置GCS访问不在本教程的范围内。以下是有一些优秀的在线指南：

- [Google：安装Cloud Storage连接器](https://cloud.google.com/dataproc/docs/concepts/connectors/install-storage-connector)
- [HortonWorks：与Google Cloud Storage一起使用](https://docs.hortonworks.com/HDPDocuments/HDP3/HDP-3.1.0/bk_cloud-data-access/content/gcp-get-started.html)
- [Cloudera：配置Google Cloud Storage连接](https://www.cloudera.com/documentation/enterprise/latest/topics/admin_gcs_config.html)

GCS访问通常在`core-site.xml`中配置，供所有使用Apache Hadoop的组件使用。

Hadoop的GCS连接器提供了一个Hadoop FileSystem的实现。遗憾的是，GCS IAM权限没有映射到Hadoop FileSystem所需的POSIX权限，因此GCS连接器呈现的是伪POSIX文件权限。

当Hive元存储访问GCS时，默认情况下，它将看到伪POSIX权限等于`0700`。如果openLooKeng和Hive元存储以不同的用户运行，会导致Hive元存储拒绝openLooKeng的数据访问。有两种可能的解决方案：

- 使用同一个用户运行openLooKeng服务和Hive服务。
- 确保Hive GCS配置包含一个值为`777`的属性`fs.gs.reported.permissions`。

## openLooKeng首次访问GCS数据

### 访问Hive元存储中已映射的数据

如果从Hive迁移到openLooKeng，则GCS数据可能已经映射到了元存储中的SQL表。在这种情况下，应该能够查询到GCS数据。

### 访问Hive元存储中尚未映射的数据

要访问Hive元存储中尚未映射的GCS数据，需要提供数据的模式、文件格式和数据位置。例如，如果在GCS桶`my_bucket`中有ORC或Parquet文件，则需要执行一个查询：

```sql
-- select schema in which the table will be defined, must already exist
USE hive.default;

-- create table
CREATE TABLE orders (
     orderkey bigint,
     custkey bigint,
     orderstatus varchar(1),
     totalprice double,
     orderdate date,
     orderpriority varchar(15),
     clerk varchar(15),
     shippriority integer,
     comment varchar(79)
) WITH (
     external_location = 'gs://my_bucket/path/to/folder',
     format = 'ORC' -- or 'PARQUET'
);
```

现在应该能够查询新映射的表：

    SELECT * FROM orders;

## 使用openLooKeng写入GCS数据

### 前提条件

在尝试向GCS写入数据之前，请确保已完成从GCS读取数据所需的所有配置。

### 创建导出模式

如果Hive元存储包含映射到GCS位置的模式，则可以使用这些模式将数据导出到GCS。如果不想使用现有的模式（或者Hive元存储中没有合适的模式），则需要创建一个新的模式：

```sql
CREATE SCHEMA hive.gcs_export WITH (location = 'gs://my_bucket/some/path');
```

### 将数据导出到GCS

在拥有了一个指向要导出数据的位置的模式后，就可以使用`CREATE TABLE AS`语句来发出导出，并选择所需的文件格式。数据将写入`gs://my_bucket/some/path/my_table`命名空间内的一个或多个文件。

示例：

```sql
CREATE TABLE hive.gcs_export.orders_export
WITH (format = 'ORC')
AS SELECT * FROM tpch.sf1.orders;
```