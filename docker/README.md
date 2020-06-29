# openLooKeng Docker Image

## About the Container
This Docker image is designed to provide the following
* An out-of-the-box single node cluster with the JMX, memory, TPC-DS, and TPC-H
 catalogs
* An image that can be deployed as a full cluster by mounting in configuration
* An image to be used as the basis for the Kubernetes openLooKeng operator

## Configuration

Configuration is expected to be mounted to either  to `/etc/hetu` or
`/usr/lib/hetu/etc` (the latter takes precedence). If neither of these exists
then the default single node configuration will be used.

### Specific Config Options

#### `node.environment`
The container supplied `run-hetu` command, upon receiving a user-defined 
node.environment, will set the config property
`node.id` to "${node.environment}-${hostname}", where the hostname is 
specific to the container, if `node.id` is not specified in the
`node.properties` file. This allows for `node.properties` to be a static file
across all worker nodes if desired. Additionally this has the added benefit of
`node.id` being consistent, predictable, and stable through restarts.

#### `node.data-dir`
The default configuration uses `/var/lib/hetu/data/` as the default for
`node.data-dir`. Thus if using the default configuration and a mounted volume
is desired for the data directory it should be mounted to `/var/lib/hetu/data/`.
