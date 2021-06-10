-- sort by is customer, item. group by item (sort aggregation will be selected)
select avg(${database}.${schema}.SortWithColNameEndWithInt.data_101),${database}.${schema}.SortWithColNameEndWithInt.data_102 from ${database}.${schema}.SortWithColNameEndWithInt
group by ${database}.${schema}.SortWithColNameEndWithInt.data_102