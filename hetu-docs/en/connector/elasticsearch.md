
Elasticsearch Connector
=======================

Overview
--------

The Elasticsearch Connector allows access to Elasticsearch data from openLooKeng. This document describes how to setup the Elasticsearch Connector to run SQL queries against Elasticsearch.

**Note**
*It is highly recommended to use Elasticsearch 6.0.0 or later.*


Configuration

To configure the Elasticsearch connector, create a catalog properties file `etc/catalog/elasticsearch.properties` with the following contents, replacing the properties as appropriate:

``` properties
connector.name=elasticsearch
elasticsearch.host=localhost
elasticsearch.port=9200
elasticsearch.default-schema-name=default
```

Configuration Properties
------------------------

The following configuration properties are available:

### `elasticsearch.host`

Hostname of the Elasticsearch node to connect to.

This property is required.

### `elasticsearch.port`

Specifies the port of the Elasticsearch node to connect to.

This property is optional; the default is 9200.

### `elasticsearch.default-schema-name`

Defines the schema that will contain all tables defined without a qualifying schema name.

This property is optional; the default is `default`.

### `elasticsearch.scroll-size`

This property defines the maximum number of hits that can be returned with each Elasticsearch scroll request.

This property is optional; the default is 1000.

### `elasticsearch.scroll-timeout`

This property defines the amount of time (ms) Elasticsearch keeps the search context alive for scroll requests

This property is optional; the default is 1m.

### `elasticsearch.request-timeout`

This property defines the timeout value for all Elasticsearch requests.

This property is optional; the default is 10s.

### `elasticsearch.connect-timeout`

This property defines the timeout value for all Elasticsearch connection attempts.

This property is optional; the default is 1s.

### `elasticsearch.max-retry-time`

This property defines the maximum duration across all retry attempts for a single request to Elasticsearch.

This property is optional; the default is 20s.

### `elasticsearch.node-refresh-interval`

This property controls how often the list of available Elasticsearch nodes is refreshed.

This property is optional; the default is 1m.

### `elasticsearch.security`

Set the authentication type to connect to Elasticsearch. For now, only support `PASSWORD`.

### `elasticsearch.auth.user`

User name to use to authenticate to Elasticsearch nodes.

### `elasticsearch.auth.password`

Password to use to authenticate to Elasticsearch nodes.

TLS Security
--------

The Elasticsearch connector provides additional security options to support Elasticsearch clusters that have been configured to use TLS.

The connector supports key stores and trust stores in PEM or Java Key Store (JKS) format. The allowed configuration values are:
### `elasticsearch.tls.keystore-path`

The path to the PEM or JKS key store. This file must be readable by the operating system user running openLooKeng.

This property is optional.

### `elasticsearch.tls.truststore-path`

The path to PEM or JKS trust store. This file must be readable by the operating system user running openLooKeng.

This property is optional.

### `elasticsearch.tls.keystore-password`

The key password for the key store specified by elasticsearch.tls.keystore-path.

This property is optional.

### `elasticsearch.tls.truststore-password`

The key password for the trust store specified by elasticsearch.tls.truststore-path.

This property is optional.

Example:

``` properties
elasticsearch.tls.enabled=true
elasticsearch.tls.keystore-path=/etc/elasticsearch/openLooKeng.jks
elasticsearch.tls.keystore-password=keystore_password
```

Data Types
--------

The data type mappings are as follows:

| Elasticsearch| openLooKeng|
|:----------|:----------|
| `binary`|  VARBINARY|
| `boolean`| BOOLEAN|
| `double`| DOUBLE|
| `float`| REAL|
| `byte`| TINYINT|
| `short`| SMALLINT|
| `integer`| INTEGER|
| `long`| BIGINT|
| `keyword`| VARCHAR|
| `text`| VARCHAR|
| `date`| TIMESTAMP|
| `ip`| IPADDRESS|
| `(all others)`| (unsupported)|

Array Type
--------
Fields in Elasticsearch can contain zero or more values , but there is no dedicated array type. To indicate a field contains an array, it can be annotated in a Presto-specific structure in the _meta section of the index mapping.

For example, you can have an Elasticsearch index that contains documents with the following structure:
### 
    {
         "array_string_field": ["presto","is","the","besto"],
         "long_field": 314159265359,
         "id_field": "564e6982-88ee-4498-aa98-df9e3f6b6109",
         "timestamp_field": "1987-09-17T06:22:48.000Z",
         "object_field": {
             "array_int_field": [86,75,309],
             "int_field": 2
         }
     }

The array fields of this structure can be defined by using the following command to add the field property definition to the _meta.presto property of the target index mapping.
### 
    curl --request PUT \
        --url localhost:9200/doc/_mapping \
        --header 'content-type: application/json' \
        --data '
    {
        "_meta": {
            "presto":{
                "array_string_field":{
                    "isArray":true
                },
                "object_field":{
                    "array_int_field":{
                        "isArray":true
                    }
                },
            }
        }
    }'
    
## Limitations
1. openLooKeng does not support to query table in Elasticsearch which has duplicated columns, such as column "name" and "NAME";
2. openLooKeng does not support to query the table in Elasticsearch that the name has special characters, such as '-', '.', etc.