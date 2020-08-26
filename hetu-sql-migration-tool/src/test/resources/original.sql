Line 1
Line 2
Line 3
Line 4
Line 5
Line 6
Line 7
Line 8
Line 9
Line 10

create EXTERNAL table IF NOT EXISTS
       p2 (id int, name string, level binary)
       comment 'test'
       partitioned by (score int, gender string)
       clustered by (id, name) sorted by (name, level) into 3 buckets
       stored as ORC
       location 'hdfs://xxx'
       TBLPROPERTIES(\"transactional\"=\"true\")