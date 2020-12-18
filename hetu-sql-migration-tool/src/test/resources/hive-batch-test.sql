CREATE SCHEMA IF NOT EXISTS S1 COMMENT 'this is a hetu schema' LOCATION '/tmp/user/mylocation';
CREATE DATABASE IF NOT EXISTS S1 LOCATION '/hive/user/warehouse/';
CREATE SCHEMA S1 COMMENT 'hetu' LOCATION '/user' WITH DBPROPERTIES(NAME='user', ID=9);

drop database S1;
drop schema S1 CASCADE;

SHOW SCHEMAS;
SHOW DATABASES;
SHOW SCHEMAS LIKE 't*';
Show schemas like 't*|a*';
Show schemas like 'test_hetu';

ALTER SCHEMA S1 SET LOCATION '/USER/HIVE';
ALTER DATABASE S1 SET OWNER USER TEST;
ALTER SCHEMA S1 SET DBPROPERTIES(NAME='HETU');

DESCRIBE SCHEMA EXTENDED TEST;

create EXTERNAL table IF NOT EXISTS p2 (id int, name string, level binary) 
comment 'test' partitioned by (score int, gender string) clustered by (id, name) sorted by (name ASC, level) into 3 buckets stored as ORC location 'hdfs://usr/hive/file' TBLPROPERTIES("transactional"="true");
CREATE TRANSACTIONAL TABLE transactional_table_test(key string, value string) PARTITIONED BY(ds string) STORED AS ORC;
CREATE TABLE IF NOT EXISTS T3 AS SELECT ID, NAME FROM T1;
create table IF NOT EXISTS p2 like p1 location 'hdfs://user/hive/mylocation';
create table t1(id1 integer UNIQUE disable novalidate, id2 integer NOT NULL,usr string DEFAULT current_user(), price double CHECK (price > 0 AND price <= 1000));
create table t2(id1 integer, id2 integer, constraint c1_unique UNIQUE(id1) disable novalidate);
create table pk(id1 int, id2 int, primary key(id1, id2) disable novalidate);
create temporary table temporary_table (id1 integer, id2 integer);
create table t1 (a struct<b:int>);
create table t1 (id int) STORED AS illegalType;

drop table IF EXISTS p2;
drop table IF EXISTS p2 purge;

CREATE VIEW V1 COMMENT 'HETU' AS SELECT (ID) FROM T1;

ALTER VIEW V1 AS SELECT * FROM T1;
ALTER VIEW V1 SET TBLPROPERTIES ("COMMENT"="HETU");

SHOW VIEWS IN D1 LIKE 'test*';

DROP VIEW TEST;

SHOW TABLES FROM TEST;
SHOW TABLES LIKE 'TEST_HETU';

SHOW CREATE TABLE T1;

ALTER TABLE T1 RENAME TO T2;
ALTER TABLE T1 SET TBLPROPERTIES ("COMMENT"="HETU");
ALTER TABLE T1 SET TBLPROPERTIES ("LOCATION"="/tmp/user/mylocation");

drop table IF EXISTS p2;
drop table IF EXISTS p2 purge;

SHOW COLUMNS IN	 DB.TB1;
SHOW COLUMNS FROM TB2 IN DB2;
SHOW COLUMNS FROM DB.TB1 LIKE 'NAME';

DESC TEST;
DESCRIBE FORMATTED DB.TB1;
DESCRIBE DEFAULT.SRC_THRIFT LINTSTRING;

ALTER TABLE TEST NOT STORED AS DIRECTORIES;
ALTER TABLE PAGE_VIEW DROP PARTITION (DT='2008-08-08', COUNTRY='US') IGNORE PROTECTION;
alter table t100 add columns (name string comment 'hetu') RESTRICT;
alter table t100 add columns (id int, name string comment 'hetu');
ALTER TABLE TEST CLUSTERED BY (ID) SORTED BY (NAME) INTO 3 BUCKETS;
ALTER TABLE TEST PARTITION (DT='2008-08-08', COUNTRY='US') SET SERDE 'ORG.APACHE.HADOOP.HIVE.SERDE2.OPENCSVSERDE' WITH SERDEPROPERTIES ('field.delim' = ',');
ALTER TABLE TEST SKEWED BY (ID) ON ('HUAWEI');
ALTER TABLE TEST NOT SKEWED;
ALTER TABLE TEST NOT STORED AS DIRECTORIES;
ALTER TABLE TEST SET SKEWED LOCATION(name='/usr/hive');
ALTER TABLE PAGE_VIEW ADD PARTITION (DT='2008-08-08', COUNTRY='US') LOCATION '/PATH/TO/US/PART080808' PARTITION (DT='2008-08-09', COUNTRY='US') LOCATION '/PATH/TO/US/PART080809';
ALTER TABLE PAGE_VIEW DROP PARTITION (DT='2008-08-08', COUNTRY='US') IGNORE PROTECTION;
ALTER TABLE PAGE_VIEW DROP PARTITION (DT='2008-08-08', COUNTRY='US') IGNORE PROTECTION;
ALTER TABLE T1 ADD CONSTRAINT TEST PRIMARY KEY (ID, NAME) DISABLE NOVALIDATE;
ALTER TABLE T1 CHANGE COLUMN ID1 ID2 INT CONSTRAINT TEST NOT NULL ENABLE;
ALTER TABLE T1 DROP CONSTRAINT TEST;

alter table t100 add columns (name string) comment 'hetu' RESTRICT;


INSERT INTO TABLE T1 VALUES(10, "NAME");
INSERT INTO TABLE T1 PARTITION(ID=10) VALUES(10, "NAME");
INSERT OVERWRITE TABLE T2 SELECT ID, NAME FROM T1;
INSERT OVERWRITE TABLE T2 PARTITION(ID=10) SELECT ID, NAME FROM T1;

UPDATE T1 SET NAME='BOO' WHERE ID=2000;

DELETE FROM T1 WHERE ID > 10;

CREATE ROLE TEST;
DROP ROLE TEST;
GRANT ROLE admin TO USER TEST;
revoke role admin from user test;
SET ROLE ALL;
grant select on t100 to user test;
REVOKE SELECT ON T100 FROM USER TEST;
SHOW GRANT ON TABLE T1;

SHOW FUNCTIONS;
SHOW FUNCTIONS LIKE 'YEAR';

EXPLAIN ANALYZE SELECT * FROM T1;
EXPLAIN AUTHORIZATION SELECT * FROM T1;

CREATE MATERIALIZED VIEW TEST STORED AS 'org.apache.hadoop.hive.druid.DruidStorageHandler' AS SELECT * FROM T1;

SELECT C1,C2 FROM T1 LATERAL VIEW EXPLODE(COL1) MYTABLE1 AS MYCOL1 LATERAL VIEW EXPLODE(COL2) MYTABLE2 AS MYCOL2;

select ~3;
select * from t1 where name is !null;
select * from t1 where id ! between 10 and 20;
select * from t1 where id not in (10,20);

SELECT C1 FROM T1 GROUP BY NAME SORT BY CNT;
SELECT C1 FROM T1 CLUSTER BY (ID, NAME);
SELECT C1 FROM T1 DISTRIBUTE BY (ID) SORT BY (NAME);
select * from t1 where name !rlike 'hetu';
select sum(b.8_0cs) from t1;

select  i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,sum(ws_ext_sales_price) as itemrevenue 
      ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
          (partition by i_class) as revenueratio
from	
	web_sales
    	,item 
    	,date_dim
where 
	ws_item_sk = i_item_sk 
  	and i_category in ('Electronics', 'Books', 'Women')
  	and ws_sold_date_sk = d_date_sk
	and d_date between cast('1998-01-06' as date) 
				and (cast('1998-01-06' as date) + 30 days)
group by 
	i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
order by 
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio
limit 100;

select  
   w_state
  ,i_item_id
  ,sum(case when (cast(d_date as date) < cast ('2000-03-18' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before
  ,sum(case when (cast(d_date as date) >= cast ('2000-03-18' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after
 from
   catalog_sales left semi join catalog_returns on
       (cs_order_number = cr_order_number 
        and cs_item_sk = cr_item_sk)
  ,warehouse 
  ,item
  ,date_dim
 where
     i_current_price between 0.99 and 1.49
 and i_item_sk          = cs_item_sk
 and cs_warehouse_sk    = w_warehouse_sk 
 and cs_sold_date_sk    = d_date_sk
 and d_date between (cast ('2000-03-18' as date) - 30 days)
                and (cast ('2000-03-18' as date) + 30 days) 
 group by
    w_state,i_item_id
 order by w_state,i_item_id
limit 100;
SELECT ID FROM T1 INNER JOIN T2;
SELECT ID FROM T1 CROSS JOIN T2;
SELECT ID FROM T1 CROSS JOIN T2 ON (ID1 = ID2);

CREATE MATERIALIZED VIEW TEST STORED AS 'org.apache.hadoop.hive.druid.DruidStorageHandler' AS SELECT * FROM T1;
DROP MATERIALIZED VIEW TEST;
ALTER MATERIALIZED VIEW TEST ENABLE REWRITE;
SHOW MATERIALIZED VIEWS IN DB1;

CREATE FUNCTION XXOO_LOWER AS 'TEST.QL.LOWERUDF' USING JAR 'HDFS:///PATH/TO/HIVE_FUNC/LOWER.JAR';
DROP FUNCTION TEST;
RELOAD FUNCTIONS;

CREATE INDEX INDEX_STUDENTID ON TABLE STUDENT_3(STUDENTID) AS 'ORG.APACHE.HADOOP.HIVE.QL.INDEX.COMPACT.COMPACTINDEXHANDLER' WITH DEFERRED REBUILD IN TABLE INDEX_TABLE_STUDENT;
DROP INDEX INDEX_STUDENTID ON STUDENT_3;
ALTER INDEX INDEX_STUDENTID ON STUDENT_3 REBUILD;
SHOW INDEX ON STUDENT_3;

SHOW PARTITIONS TEST_PARTITION WHERE ID > 10 LIMIT 3;
CREATE TEMPORARY MACRO STRING_LEN_PLUS_TWO(X STRING) LENGTH(X) + 2;
DROP TEMPORARY MACRO TEST;

SHOW ROLE GRANT USER TEST;
SHOW PRINCIPALS TEST;

INSERT OVERWRITE DIRECTORY '/USER/HIVE/TEST' SELECT * FROM T1;
show locks t_real_user;
SHOW CONF 'hive.exec.parallel';
SHOW TRANSACTIONS;
SHOW COMPACTIONS;
ABORT TRANSACTIONS 0001;
LOAD DATA LOCAL INPATH '/PATH/TO/LOCAL/FILES' OVERWRITE  INTO TABLE TEST PARTITION (COUNTRY='CHINA');
EXPORT TABLE DEPARTMENT TO 'HDFS_EXPORTS_LOCATION/DEPARTMENT';
IMPORT TABLE IMPORTED_DEPT FROM 'HDFS_EXPORTS_LOCATION/DEPARTMENT';

