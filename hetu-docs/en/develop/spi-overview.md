
# SPI Overview

When you implement a new openLooKeng plugin, you implement interfaces and override methods defined by the SPI. Plugins can provide additional [Connectors](./connectors.md), [Types](./types.md), [Functions](./functions.md) and [System Access Control](./system-access-control.md). In particular, connectors are the source of all data for queries in openLooKeng: they back each catalog available to openLooKeng.

## Code

The SPI source can be found in the `presto-spi` directory in the root of the openLooKeng source tree.

## Plugin Metadata

Each plugin identifies an entry point: an implementation of the `Plugin`interface. This class name is provided to openLooKeng via the standard Java `ServiceLoader` interface: the classpath contains a resource file named `io.prestosql.spi.Plugin` in the `META-INF/services` directory. The content of this file is a single line listing the name of the plugin class:

``` 
com.example.plugin.ExamplePlugin
```

For a built-in plugin that is included in the openLooKeng source code, this resource file is created whenever the `pom.xml` file of a plugin contains the following line:

``` xml
<packaging>hetu-plugin</packaging>
```

## Plugin

The `Plugin` interface is a good starting place for developers looking to understand the openLooKeng SPI. It contains access methods to retrieve various classes that a Plugin can provide. For example, the `getConnectorFactories()` method is a top-level function that openLooKeng calls to retrieve a `ConnectorFactory` when openLooKeng is ready to create an instance of a connector to back a catalog. There are similar methods for `Type`, `ParametricType`, `Function`, `SystemAccessControl`, and `EventListenerFactory` objects.

## Building Plugins via Maven

Plugins depend on the SPI from openLooKeng:

``` xml
<dependency>
    <groupId>io.prestosql</groupId>
    <artifactId>presto-spi</artifactId>
    <scope>provided</scope>
</dependency>
```

The plugin uses the Maven `provided` scope because openLooKeng provides the classes from the SPI at runtime and thus the plugin should not include them in the plugin assembly.

There are a few other dependencies that are provided by openLooKeng, including Slice and Jackson annotations. In particular, Jackson is used for serializing connector handles and thus plugins must use the annotations version provided by openLooKeng.

All other dependencies are based on what the plugin needs for its own implementation. Plugins are loaded in a separate class loader to provide isolation and to allow plugins to use a different version of a library that openLooKeng uses internally.

For an example `pom.xml` file, see the example HTTP connector in the `presto-example-http` directory in the root of the openLooKeng source tree.

## Deploying a Custom Plugin

In order to add a custom plugin to a openLooKeng installation, create a directory for that plugin in the openLooKeng plugin directory and add all the necessary jars for the plugin to that directory. For example, for a plugin called `my-functions`, you would create a directory `my-functions` in the openLooKeng plugin directory and add the relevant jars to that directory.

By default, the plugin directory is the `plugin` directory relative to the directory in which openLooKeng is installed, but it is configurable using the configuration variable `catalog.config-dir`. In order for openLooKeng to pick up the new plugin, you must restart openLooKeng.

Plugins must be installed on all nodes in the openLooKeng cluster (coordinator and workers).
