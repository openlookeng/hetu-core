select count(${database}.${schema}.web_returns_partition_bucketCount1.wr_return_quantity),${database}.${schema}.web_returns_partition_bucketCount1.wr_return_quantity from ${database}.${schema}.web_returns_partition_bucketCount1
group by ${database}.${schema}.web_returns_partition_bucketCount1.wr_return_quantity
