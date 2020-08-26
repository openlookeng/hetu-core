Line 2
Line 3 with changes
Line 4
Line 5 with changes and
a new line
Line 6
new line 6.1
Line 7
Line 8
Line 9
Line 10 with changes

CREATE TABLE IF NOT EXISTS p2 (
        id int,
        name string,
        level varbinary,
        score int,
        gender string
        )
        COMMENT 'test'
        WITH (
        partitioned_by = ARRAY['score','gender'],
        bucketed_by = ARRAY['id','name'],
        sorted_by = ARRAY['name','level'],
        bucket_count = 3,
        format = 'ORC',
        external = true,
        location = 'hdfs://xxx',
        transactional = true
        );