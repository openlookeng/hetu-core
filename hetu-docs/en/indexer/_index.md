+++
bookCollapseSection = "true"
weight = 8
bookToc = false
title = "Heuristic Indexer"
+++


# Heuristic Indexer

Indexes can be created using one or more columns of a database table, providing faster random lookups. Most Big Data formats such as ORC, Parquet and CarbonData already have indices embedded in them.

The Heuristic Indexer allows creating indexes on existing data but stores the index external to the original data source. This provides several benefits:

  - The index is agnostic to the underlying data source and can be used by any query engine
  - Existing data can be indexed without having to rewrite the existing data files
  - New index types not supported by the underlying data source can be created
  - Index data does not use the storage space of the data source