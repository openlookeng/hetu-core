+++
weight = 1
title = "openLooKeng Heuristic Indexer"
+++

# openLooKeng Heuristic Indexer

## Introduction

Indexes can be created using one or more columns of a database table, providing faster random lookups. Most Big Data formats such as ORC, Parquet and CarbonData already have indices embedded in them.

The Heuristic Indexer allows creating indexes on existing data but stores the index external to the original data source. This provides several benefits:

  - The index is agnostic to the underlying data source and can be used by any query engine
  - Existing data can be indexed without having to rewrite the existing data files
  - New index types not supported by the underlying data source can be created
  - Index data does not use the storage space of the data source

## Example Usecases

### 1. Filtering scheduled Splits during query execution

When the engine needs to schedule a TableScan operation, it schedules Splits on the workers. These Splits are responsible for reading a portion of the source data. However, not all Splits will return data if a predicate is applied.

By keeping an external index for the predicate column, the Heuristic Indexer can determine whether each split contains the values being searched for and only schedule the read operation for the splits which possibly contain the value.

![indexer_filter_splits](../images/indexer_filter_splits.png)

### 2. Filtering Block early when reading ORC files

When data needs to be read from an ORC file, the ORCRecordReader is used. This reader reads data from Stripes as batches (e.g. 1024 rows), which then form Pages. However, if a predicate is present, not all entries in the batch are required, some may be filtered out later by the Filter operator.

By keeping an external bitmap index for the predicate column, the Heuristic Indexer can filter out rows which do not match the predicates before the Filter operator is even applied.

