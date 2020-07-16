
# Heuristic indexer

## `hetu-heuristic-index-cli` module

This module is for CLI access towards heuristic indexer. Any changes made to index CLI should happen here.

#### IndexerCommand

Exposes the indexer as an executable and takes parameters such as:
output filesystem and location, index type, table, columns, and partitions. Provides a help dialog with usage info.

## `hetu-heuristic-index` module

This is the module holding all heuristic-index implementations excluding CLI. They are divided into two packages:

  - Core components: factories, index accessors/mutators, constants, etc. Currently placed in `io.hetu.core.heuristicindex`.
  - Plugins: implementations of different data sources and indices. Currently placed in `io.hetu.core.plugin.heuristicindex`.

### Core components

The core components of heuristic index are `IndexFactory`, `IndexClient` and `IndexWriter`.

#### HeuristicIndexFactory

  - Takes parameters such as table, columns, datasource, index stores, and index types and creates an Indexer which is configured for the corresponding Datasource, IndexStore and Index.
  - Will identify the datasource type from the fully qualified table name
  - For datasources such as HDFS, the builder will also query the table metadata and identify the format of the data files, e.g. ORC, Parquet and create the corresponding Datasource Will also read the configuration values from the catalog files to identify the output IndexStore
  - Set the specified Index types
  
  It is the only implementation of `IndexFactory` interface.

#### HeuristicIndexWriter

  - The IndexWriter will have the required Datasource, IndexStore and Index injected to it by the  IndexerFactory
  - It will call the Datasource to read the records, then have the Index create the specified index and then persist it to the IndexStore
  
  It is the only implementation of `IndexWriter` interface.


#### HeuristicIndexClient

  - Helps with reading the Index files from the IndexStore
  - Allows deleting and showing Index files
  
  It is the only implementation of `IndexClient` interface.

### Plugins

Heuristic indexer supports plugins for new data sources and custom indices. 

#### DataSource

  - The interface which all supported data sources must implement
  - This will allow us to plug in any data source as long as it
    implements the required operations such as reading splits
  - Each implementation is a plugin

##### HiveDataSource

  - The Datasource implementation for a Hive table
  - This implementation will check the table's metadata and then delegate the work to the ORC or Parquet data source (currently only ORC is supported).

##### HdfsOrcDataSource

  - The Datasource implementation for HDFS ORC files
  - Reads ORC files and creates a split for every stripe in the file

#### Index

  - Represents a generic index type
  - Provides abstract methods for each index type to implement
  - Each implementation is a plugin

#### BloomIndex

  - Creates Index based on BloomFilter

#### BitmapIndex

  - Creates Index based on Bitmap index

#### MinMaxIndex

  - This Index simply stores the min and max values in the split
