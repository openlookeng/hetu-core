
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

Allows setting password security to authenticate to Elasticsearch.

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
| `(all others)`| (unsupported)|
