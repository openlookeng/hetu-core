+++

weight = 10

title = "Heuristic indexer"
+++

# Heuristic indexer

## Components

### IndexerCommand

Exposes the indexer as an executable and takes parameters such as:
output filesystem and location, index type, table, columns, and partitions. Provides a help dialog with usage info.

### IndexerFactory

  - Takes parameters such as table, columns, datasource, index stores, and index types and creates an Indexer which is configured for the corresponding Datasource, IndexStore and Index.
  - Will identify the datasource type from the fully qualified table name
  - For datasources such as HDFS, the builder will also query the table metadata and identify the format of the data files, e.g. ORC, Parquet and create the corresponding Datasource Will also read the configuration values from the catalog files to identify the output IndexStore
  - Set the specified Index types

### IndexWriter

  - The IndexWriter will have the required Datasource, IndexStore and Index injected to it by the  IndexerFactory
  - It will call the Datasource to read the records, then have the Index create the specified index and then persist it to the IndexStore

### IndexClient

  - Helps with reading the Index files from the IndexStore
  - Allows deleting and showing Index files

### DataSource

  - The interface which all supported data sources must implement
  - This will allow us to plug in any data source as long as it
    implements the required operations such as reading splits
  - Each implementation is a plugin

### HiveDataSource

  - The Datasource implementation for a Hive table
  - This implementation will check the table's metadata and then delegate the work to the ORC or Parquet data source (currently only ORC is supported).

### HdfsOrcDataSource

  - The Datasource implementation for HDFS ORC files
  - Reads ORC files and creates a split for every stripe in the file

### IndexStore

  - Represents a generic store for storing indexes
  - Provides abstract read/write methods for each store to implement
  - Each implementation is a plugin

### LocalIndexStore

  - Supports writing index to the local file system

### HdfsIndexStore

  - Supports writing index to HDFS

### Index

  - Represents a generic index type
  - Provides abstract methods for each index type to implement
  - Each implementation is a plugin

### BloomIndex

  - Creates Index based on BloomFilter

### BitmapIndex

  - Creates Index based on Bitmap index

### MinMaxIndex

  - This Index simply stores the min and max values in the split
