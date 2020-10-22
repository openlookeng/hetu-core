/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hetu.core.sql.migration.tool;

import io.hetu.core.sql.migration.SqlSyntaxType;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.parser.StatementSplitter;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.io.Files.asCharSource;
import static io.hetu.core.sql.migration.Constants.FAILED;
import static io.hetu.core.sql.migration.Constants.MESSAGE;
import static io.hetu.core.sql.migration.Constants.SUCCESS;
import static io.hetu.core.sql.migration.Constants.UNSUPPORTED;
import static io.hetu.core.sql.migration.Constants.WARNING;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHiveSqlMigrate
{
    private SqlParser sqlParser;
    private ParsingOptions parsingOptions;
    private SqlSyntaxConverter sqlConverter;

    @BeforeClass
    public void setup() throws Exception
    {
        sqlParser = new SqlParser();
        parsingOptions = new io.prestosql.sql.parser.ParsingOptions(DecimalLiteralTreatment.AS_DECIMAL);
        SessionProperties sessionProperties = new SessionProperties();
        sessionProperties.setSourceType(SqlSyntaxType.HIVE);
        sessionProperties.setParsingOptions(false);
        sqlConverter = SqlConverterFactory.getSqlConverter(sessionProperties);
    }

    @Test
    public void testCreateSchema() throws Exception
    {
        String sql = "CREATE SCHEMA IF NOT EXISTS S1 COMMENT 'hetu' LOCATION '/user'";
        String expectedSql = "CREATE SCHEMA IF NOT EXISTS S1\n" +
                "WITH (\n" +
                "   location = '/user'\n" +
                ")";
        assertWarning(sql, expectedSql);

        String sql2 = "CREATE SCHEMA IF NOT EXISTS S1 COMMENT 'hetu' LOCATION '/user' WITH DBPROPERTIES(NAME='user', ID=9)";
        assertUnsupported(sql2, Optional.of("WITH DBPROPERTIES"));
    }

    @Test
    public void testDropSchema() throws Exception
    {
        String sql1 = "drop schema S1 RESTRICT";
        String expectedSql = "DROP SCHEMA S1 RESTRICT";
        assertSuccess(sql1, expectedSql);

        String sql2 = "drop schema S1 CASCADE";
        assertUnsupported(sql2, Optional.of("CASCADE"));
    }

    @Test
    public void testAlterSchema()
    {
        String sql1 = "ALTER SCHEMA S1 SET LOCATION '/USER/HIVE'";
        assertUnsupported(sql1, Optional.of("ALTER SCHEMA"));

        String sql2 = "ALTER SCHEMA S1 SET OWNER USER TEST";
        assertUnsupported(sql2, Optional.of("ALTER SCHEMA"));

        String sql3 = "ALTER SCHEMA S1 SET DBPROPERTIES(NAME='HETU')";
        assertUnsupported(sql3, Optional.of("ALTER SCHEMA"));
    }

    @Test
    public void testDescribeSchema()
    {
        String sql = "DESCRIBE SCHEMA EXTENDED TEST";
        assertUnsupported(sql, Optional.of("DESCRIBE SCHEMA"));

        sql = "DESCRIBE DATABASE TEST";
        assertUnsupported(sql, Optional.of("DESCRIBE DATABASE"));
    }

    @Test
    public void testCreateTable() throws Exception
    {
        String sql = "create EXTERNAL table IF NOT EXISTS \n" +
                "p2 (id int, name string, level binary) \n" +
                "comment 'test' \n" +
                "partitioned by (score int, gender string) \n" +
                "clustered by (id, name) sorted by (name ASC, level) into 3 buckets \n" +
                "stored as ORC \n" +
                "location 'hdfs://xxx' \n" +
                "TBLPROPERTIES(\"transactional\"=\"true\")";

        String expectedSql = "CREATE TABLE IF NOT EXISTS p2 (\n" +
                "   id int,\n" +
                "   name string,\n" +
                "   level varbinary,\n" +
                "   score int,\n" +
                "   gender string\n" +
                ")\n" +
                "COMMENT 'test'\n" +
                "WITH (\n" +
                "   partitioned_by = ARRAY['score','gender'],\n" +
                "   bucketed_by = ARRAY['id','name'],\n" +
                "   sorted_by = ARRAY['name ASC','level'],\n" +
                "   bucket_count = 3,\n" +
                "   format = 'ORC',\n" +
                "   external = true,\n" +
                "   location = 'hdfs://xxx',\n" +
                "   transactional = true\n" +
                ")";
        assertSuccess(sql, expectedSql);

        String sql2 = "create table constraints1(id1 integer UNIQUE disable novalidate, id2 integer NOT NULL,usr string DEFAULT current_user(), price double CHECK (price > 0 AND price <= 1000))";
        assertUnsupported(sql2, Optional.of("UNIQUE"));

        String sql3 = "create table constraints2(id1 integer, id2 integer, constraint c1_unique UNIQUE(id1) disable novalidate)";
        assertUnsupported(sql3, Optional.of("CONSTRAINT"));

        String sql4 = "create table constraints3(id1 integer, id2 integer,constraint c1_check CHECK(id1 + id2 > 0))";
        assertUnsupported(sql4, Optional.of("CONSTRAINT"));

        String sql5 = "create temporary table temporary_table (id1 integer, id2 integer)";
        assertUnsupported(sql5, Optional.of("TEMPORARY"));

        String sql6 = "CREATE TRANSACTIONAL TABLE transactional_table_test(key string comment 'key', value string comment 'value') PARTITIONED BY(ds string) STORED AS ORC";
        expectedSql = "CREATE TABLE transactional_table_test (\n" +
                "   key string COMMENT 'key',\n" +
                "   value string COMMENT 'value',\n" +
                "   ds string\n" +
                ")\n" +
                "WITH (\n" +
                "   transactional = true,\n" +
                "   partitioned_by = ARRAY['ds'],\n" +
                "   format = 'ORC'\n" +
                ")";
        assertSuccess(sql6, expectedSql);

        String sql7 = "create table constraints7(id1 integer, id2 integer primary key)";
        assertUnsupported(sql7, Optional.of("PRIMARY KEY"));

        String sql8 = "create table constraints8(id1 integer, id2 integer not null)";
        assertUnsupported(sql8, Optional.of("NOT NULL"));

        String sql9 = "create table constraints9(id1 integer, id2 integer default null)";
        assertUnsupported(sql9, Optional.of("DEFAULT"));

        String sql10 = "create table constraints9(id1 integer, id2 integer check(id2 > 10))";
        assertUnsupported(sql10, Optional.of("CHECK"));

        String sql11 = "create table constraints9(id1 integer, id2 integer) skewed by (id1) on (1,3)";
        assertUnsupported(sql11, Optional.of("SKEWED"));

        String sql12 = "create table constraints9(id1 integer, id2 integer) row format delimited";
        assertUnsupported(sql12, Optional.of("ROW FORMAT"));

        String sql13 = "create table constraints9(id1 integer, id2 integer) stored by 'test'";
        assertUnsupported(sql13, Optional.of("STORED BY"));

        String sql14 = "create external table constraints9(id1 integer, id2 integer)";
        assertUnsupported(sql14, Optional.of("EXTERNAL"));

        String sql15 = "create EXTERNAL table IF NOT EXISTS \n" +
                "p2 (id int, name string, level binary) \n" +
                "comment 'test' \n" +
                "partitioned by (score int, gender string) \n" +
                "clustered by (id, name) sorted by (name ASC, level) into 3 buckets \n" +
                "stored as ORC \n" +
                "location 'hdfs://xxx' \n" +
                "TBLPROPERTIES(\"debug\"=\"true\")";
        assertUnsupported(sql15, Optional.of("TBLPROPERTIES"));
    }

    @Test
    public void testCreateTableWithIllegalType() throws Exception
    {
        String sql1 = "create table t1 (a struct<b:int>)";
        assertUnsupported(sql1, Optional.of("struct"));

        String sql2 = "create table t1 (a uniontype<int, string>)";
        assertFailed(sql2);
    }

    @Test
    public void testCreateTableWithFileFormat() throws Exception
    {
        String sql1 = "create table t1 (id int) STORED AS orc";
        String expectedSql = "CREATE TABLE t1 (\n" +
                "   id int\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ")";
        assertSuccess(sql1, expectedSql);

        String sql2 = "create table t1 (id int) STORED AS illegalType";
        assertUnsupported(sql2, Optional.of("illegalType"));
    }

    @Test
    public void testCreateTableLike() throws Exception
    {
        String sql = "create table IF NOT EXISTS p2 like p1 location 'hdfs://xxx'";
        String expectedSql = "CREATE TABLE IF NOT EXISTS p2 (\n" +
                "   LIKE p1 EXCLUDING PROPERTIES\n" +
                ")\n" +
                "WITH (\n" +
                "   location = 'hdfs://xxx'\n" +
                ")";
        assertSuccess(sql, expectedSql);

        sql = "create external table p3 like p1 location 'hdfs://xxx'";
        expectedSql = "CREATE TABLE p3 (\n" +
                "   LIKE p1 EXCLUDING PROPERTIES\n" +
                ")\n" +
                "WITH (\n" +
                "   external = true,\n" +
                "   location = 'hdfs://xxx'\n" +
                ")";
        assertSuccess(sql, expectedSql);

        sql = "create external table p3 like p1";
        assertUnsupported(sql, Optional.of("EXTERNAL"));
    }

    @Test
    public void testCreateTableSubquery() throws Exception
    {
        String sql = "CREATE TABLE IF NOT EXISTS T3 AS SELECT ID, NAME FROM T1";
        String expectedSql = "CREATE TABLE IF NOT EXISTS T3 AS SELECT\n" +
                "  ID\n" +
                ", NAME\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql, expectedSql);

        sql = "CREATE TABLE T3 COMMENT 'hetu' AS SELECT ID, NAME FROM T1";
        expectedSql = "CREATE TABLE T3\n" +
                "COMMENT 'hetu' AS SELECT\n" +
                "  ID\n" +
                ", NAME\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql, expectedSql);

        sql = "CREATE TABLE T3 (ID, NAME) AS SELECT ID, NAME FROM T1";
        expectedSql = "CREATE TABLE T3( ID, NAME ) AS SELECT\n" +
                "  ID\n" +
                ", NAME\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql, expectedSql);

        sql = "CREATE TABLE T3 LOCATION '/USR/HIVE/TEST' AS SELECT ID, NAME FROM T1";
        expectedSql = "CREATE TABLE T3\n" +
                "WITH (\n" +
                "   location = '/USR/HIVE/TEST'\n" +
                ") AS SELECT\n" +
                "  ID\n" +
                ", NAME\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql, expectedSql);

        sql = "CREATE TRANSACTIONAL TABLE T3 AS SELECT ID, NAME FROM T1";
        expectedSql = "CREATE TABLE T3\n" +
                "WITH (\n" +
                "   transactional = true\n" +
                ") AS SELECT\n" +
                "  ID\n" +
                ", NAME\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql, expectedSql);

        sql = "CREATE TABLE T3 STORED AS ORC AS SELECT ID, NAME FROM T1";
        expectedSql = "CREATE TABLE T3\n" +
                "WITH (\n" +
                "   format = 'ORC'\n" +
                ") AS SELECT\n" +
                "  ID\n" +
                ", NAME\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql, expectedSql);

        sql = "CREATE TABLE T3 TBLPROPERTIES('transactional'='true') AS SELECT ID, NAME FROM T1";
        expectedSql = "CREATE TABLE T3\n" +
                "WITH (\n" +
                "   transactional = true\n" +
                ") AS SELECT\n" +
                "  ID\n" +
                ", NAME\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql, expectedSql);

        sql = "CREATE TEMPORARY TABLE T3 AS SELECT ID, NAME FROM T1";
        assertUnsupported(sql, Optional.of("TEMPORARY"));

        sql = "CREATE TABLE T3 TBLPROPERTIES('debug'='true') AS SELECT ID, NAME FROM T1";
        assertUnsupported(sql, Optional.of("TBLPROPERTIES"));
    }

    @Test
    public void testDropTable() throws Exception
    {
        String sql1 = "drop table IF EXISTS p2";
        String expectedSql = "DROP TABLE IF EXISTS p2";
        assertSuccess(sql1, expectedSql);

        String sql2 = "drop table IF EXISTS p2 purge";
        assertUnsupported(sql2, Optional.of("PURGE"));
    }

    @Test
    public void testInsertInto() throws Exception
    {
        String sql1 = "INSERT INTO TABLE T1 VALUES(10, \"NAME\")";
        String expectedSql1 = "INSERT INTO t1\n" +
                " VALUES \n" +
                "  ROW (10, 'NAME')\n";
        assertSuccess(sql1, expectedSql1);

        String sql2 = "INSERT INTO TABLE T2 SELECT ID, NAME FROM T1";
        String expectedSql2 = "INSERT INTO t2\n" +
                "SELECT\n" +
                "  ID\n" +
                ", NAME\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql2, expectedSql2);
    }

    @Test
    public void testInsertOverwrite() throws Exception
    {
        String sql = "INSERT OVERWRITE TABLE T2 SELECT ID, NAME FROM T1";
        String expectedSql = "INSERT OVERWRITE t2\n" +
                "SELECT\n" +
                "  ID\n" +
                ", NAME\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testUpdate() throws Exception
    {
        String sql = "UPDATE T1 SET NAME='BOO' WHERE ID=2000";
        String expectedSql = "UPDATE t1 SET\n" +
                "name='BOO'\n" +
                "WHERE (ID = 2000)\n";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testDelete() throws Exception
    {
        String sql = "DELETE FROM T1 WHERE ID > 10";
        String expectedSql = "DELETE FROM T1 WHERE (ID > 10)";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testAlterTable() throws Exception
    {
        String sql1 = "ALTER TABLE T1 RENAME TO T2";
        String expectedSql1 = "ALTER TABLE t1 RENAME TO t2";
        assertSuccess(sql1, expectedSql1);

        String sql2 = "ALTER TABLE T1 SET TBLPROPERTIES (\"COMMENT\"=\"HETU\")";
        String expectedSql2 = "COMMENT ON TABLE t1 IS 'HETU'";
        assertSuccess(sql2, expectedSql2);
    }

    @Test
    public void testAlterTableUnsupported()
    {
        String sql1 = "ALTER TABLE TEST PARTITION (DT='2008-08-08', COUNTRY='US') SET SERDE 'ORG.APACHE.HADOOP.HIVE.SERDE2.OPENCSVSERDE' WITH SERDEPROPERTIES ('field.delim' = ',')";
        assertUnsupported(sql1, Optional.of("SET SERDE"));

        String sql2 = "ALTER TABLE TEST CLUSTERED BY (ID) SORTED BY (NAME) INTO 3 BUCKETS";
        assertUnsupported(sql2, Optional.of("STORAGE PROPERTIES"));

        String sql3 = "ALTER TABLE TEST SKEWED BY (ID) ON ('HUAWEI')";
        assertUnsupported(sql3, Optional.of("SKEWED"));

        String sql4 = "ALTER TABLE TEST NOT SKEWED";
        assertUnsupported(sql4, Optional.of("NOT SKEWED"));

        String sql5 = "ALTER TABLE TEST NOT STORED AS DIRECTORIES";
        assertUnsupported(sql5, Optional.of("NOT STORED AS DIRECTORIES"));

        String sql6 = "ALTER TABLE TEST SET SKEWED LOCATION(name='/usr/hive')";
        assertUnsupported(sql6, Optional.of("SET SKEWED LOCATION"));

        String sql7 = "ALTER TABLE PAGE_VIEW ADD PARTITION (DT='2008-08-08', COUNTRY='US') LOCATION '/PATH/TO/US/PART080808' PARTITION (DT='2008-08-09', COUNTRY='US') LOCATION '/PATH/TO/US/PART080809'";
        assertUnsupported(sql7, Optional.of("ADD PARTITION"));

        String sql8 = "ALTER TABLE PAGE_VIEW DROP PARTITION (DT='2008-08-08', COUNTRY='US') IGNORE PROTECTION";
        assertUnsupported(sql8, Optional.of("DROP PARTITION"));

        String sql9 = "ALTER TABLE T1 ADD CONSTRAINT TEST PRIMARY KEY (ID, NAME) DISABLE NOVALIDATE";
        assertUnsupported(sql9, Optional.of("ADD CONSTRAINT"));

        String sql10 = "ALTER TABLE FOO PARTITION (DS='2008-04-08', HR) CHANGE COLUMN DEC_COLUMN_NAME DEC_COLUMN_NAME DECIMAL(38,18)";
        assertUnsupported(sql10, Optional.of("CHANGE COLUMN"));

        String sql11 = "ALTER TABLE T1 DROP CONSTRAINT TEST";
        assertUnsupported(sql11, Optional.of("DROP CONSTRAINT"));

        String sql12 = "ALTER TABLE D1.T1 UNSET SERDEPROPERTIES('FIELD.DELIM')";
        assertUnsupported(sql12, Optional.of("UNSET SERDEPROPERTIES"));

        String sql13 = "ALTER TABLE D1.T1 PARTITION (ID=10) RENAME TO PARTITION(NAME='HETU')";
        assertUnsupported(sql13, Optional.of("RENAME PARTITION"));

        String sql14 = "ALTER TABLE D1.T1 EXCHANGE PARTITION (ID=10) WITH TABLE D1.T2";
        assertUnsupported(sql14, Optional.of("EXCHANGE PARTITION"));

        String sql15 = "ALTER TABLE D1.T1 RECOVER PARTITIONS";
        assertUnsupported(sql15, Optional.of("RECOVER PARTITION"));

        String sql16 = "ALTER TABLE D1.T1 ARCHIVE PARTITION(NAME='HETU')";
        assertUnsupported(sql16, Optional.of("ARCHIVE PARTITION"));

        String sql17 = "ALTER TABLE D1.T1 UNARCHIVE PARTITION(NAME='HETU')";
        assertUnsupported(sql17, Optional.of("UNARCHIVE PARTITION"));

        String sql18 = "ALTER TABLE D1.T1 SET FILEFORMAT ORC";
        assertUnsupported(sql18, Optional.of("SET FILEFORMAT"));

        String sql19 = "ALTER TABLE D1.T1 SET LOCATION 'usr/hive/test'";
        assertUnsupported(sql19, Optional.of("SET LOCATION"));

        String sql20 = "ALTER TABLE D1.T1 TOUCH";
        assertUnsupported(sql20, Optional.of("TOUCH PARTITION"));

        String sql21 = "ALTER TABLE D1.T1 PARTITION(NAME='HETU') ENABLE NO_DROP";
        assertUnsupported(sql21, Optional.of("PARTITION PROTECTION"));

        String sql22 = "ALTER TABLE D1.T1 PARTITION(NAME='HETU') COMPACT MAJOR";
        assertUnsupported(sql22, Optional.of("PARTITION COMPACT"));

        String sql23 = "ALTER TABLE D1.T1 PARTITION(NAME='HETU') CONCATENATE";
        assertUnsupported(sql23, Optional.of("PARTITION CONCATENATE"));

        String sql24 = "ALTER TABLE D1.T1 PARTITION(NAME='HETU') UPDATE COLUMNS";
        assertUnsupported(sql24, Optional.of("UPDATE COLUMNS"));

        String sql25 = "alter table t100 partition(name='hetu') add columns (name string comment 'hetu')";
        assertUnsupported(sql25, Optional.of("PARTITION"));

        String sql26 = "alter table t100 replace columns (name string comment 'hetu')";
        assertUnsupported(sql26, Optional.of("REPLACE"));

        String sql27 = "alter table t100 add columns (name string comment 'hetu') cascade";
        assertUnsupported(sql27, Optional.of("CASCADE"));
    }

    @Test
    public void testShowTableExtended()
    {
        String sql = "show table extended like part_table";
        assertUnsupported(sql, Optional.of("SHOW TABLE EXTENDED"));
    }

    @Test
    public void testShowTableProperties()
    {
        String sql = "SHOW TBLPROPERTIES TBLNAME('FOO')";
        assertUnsupported(sql, Optional.of("SHOW TBLPROPERTIES"));
    }

    @Test
    public void testTruncateTable()
    {
        String sql = "TRUNCATE TABLE TEST";
        assertUnsupported(sql, Optional.of("TRUNCATE TABLE"));
    }

    @Test
    public void testMsckRepairTable()
    {
        String sql = "MSCK REPAIR TABLE TEST ADD PARTITIONS";
        assertUnsupported(sql, Optional.of("MSCK"));
    }

    @Test
    public void testAddColumn() throws Exception
    {
        String sql = "alter table t100 add columns (name string comment 'hetu') RESTRICT";
        String expectedSql = "ALTER TABLE t100 ADD COLUMN name string COMMENT 'hetu'";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testCreateView() throws Exception
    {
        String sql = "CREATE VIEW V1 COMMENT 'HETU' AS SELECT (ID) FROM T1";
        String expectedSql = "CREATE VIEW V1 AS\n" +
                "SELECT ID\n" +
                "FROM\n" +
                "  T1\n";
        assertWarning(sql, expectedSql);

        sql = "CREATE VIEW V1 (ID, NAME) COMMENT 'HETU' AS SELECT (ID) FROM T1";
        assertUnsupported(sql, Optional.of("COLUMN ALIASES"));

        sql = "CREATE VIEW V1 COMMENT 'HETU' TBLPROPERTIES(NAME='HETU') AS SELECT (ID) FROM T1";
        assertUnsupported(sql, Optional.of("TBLPROPERTIES"));
    }

    @Test
    public void testDropView() throws Exception
    {
        String sql = "DROP VIEW TEST";
        String expectedSql = "DROP VIEW test";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testAlterView() throws Exception
    {
        String sql1 = "ALTER VIEW V1 AS SELECT * FROM T1";
        String expectedSql1 = "CREATE OR REPLACE VIEW V1 AS\n" +
                "SELECT *\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql1, expectedSql1);

        String sql2 = "ALTER VIEW V1 SET TBLPROPERTIES (\"COMMENT\"=\"HETU\")";
        assertUnsupported(sql2, Optional.of("SET TBLPROPERTIES"));
    }

    @Test
    public void testShowViews()
    {
        String sql = "SHOW VIEWS IN D1 LIKE 'test*'";
        assertUnsupported(sql, Optional.of("SHOW VIEWS"));
    }

    @Test
    public void testCreateRole() throws Exception
    {
        String sql = "CREATE ROLE TEST";
        String expectedSql = "CREATE ROLE TEST";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testDropRole() throws Exception
    {
        String sql = "DROP ROLE TEST";
        String expectedSql = "DROP ROLE TEST";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testGrantRole() throws Exception
    {
        String sql = "GRANT ROLE admin TO USER TEST";
        String expectedSql = "GRANT admin TO USER TEST";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testRevokeRole() throws Exception
    {
        String sql = "revoke role admin from user test";
        String expectedSql = "REVOKE admin FROM USER test";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testSetRole() throws Exception
    {
        String sql = "SET ROLE ALL";
        String expectedSql = "SET ROLE ALL";
        assertSuccess(sql, expectedSql);

        sql = "SET ROLE NONE";
        expectedSql = "SET ROLE NONE";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testGrant() throws Exception
    {
        String sql = "grant select on t100 to user test";
        String expectedSql = "GRANT select ON TABLE t100 TO USER test";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testRevoke() throws Exception
    {
        String sql = "REVOKE SELECT ON T100 FROM USER TEST";
        String expectedSql = "REVOKE SELECT ON TABLE t100 FROM USER TEST";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testShowGrants() throws Exception
    {
        String sql = "SHOW GRANT ON TABLE T1";
        String expectedSql = "SHOW GRANTS ON TABLE t1";
        assertSuccess(sql, expectedSql);

        sql = "SHOW GRANT USER ON TABLE T1";
        assertUnsupported(sql, Optional.of("PRINCIPAL"));

        sql = "SHOW GRANT ON ALL";
        assertUnsupported(sql, Optional.of("ALL"));
    }

    @Test
    public void testExplain() throws Exception
    {
        String sql = "EXPLAIN ANALYZE SELECT * FROM T1";
        String expectedSql = "EXPLAIN ANALYZE \n" +
                "SELECT *\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql, expectedSql);

        sql = "EXPLAIN EXTENDED SELECT * FROM T1";
        assertUnsupported(sql, Optional.of("EXTENDED"));

        sql = "EXPLAIN CBO SELECT * FROM T1";
        assertUnsupported(sql, Optional.of("CBO"));

        sql = "EXPLAIN AST SELECT * FROM T1";
        assertUnsupported(sql, Optional.of("AST"));

        sql = "EXPLAIN DEPENDENCY SELECT * FROM T1";
        assertUnsupported(sql, Optional.of("DEPENDENCY"));

        sql = "EXPLAIN AUTHORIZATION SELECT * FROM T1";
        assertUnsupported(sql, Optional.of("AUTHORIZATION"));

        sql = "EXPLAIN LOCKS SELECT * FROM T1";
        assertUnsupported(sql, Optional.of("LOCKS"));

        sql = "EXPLAIN VECTORIZATIONANALYZE SELECT * FROM T1";
        assertUnsupported(sql, Optional.of("VECTORIZATIONANALYZE"));
    }

    @Test
    public void testShowCreateTable() throws Exception
    {
        String sql = "SHOW CREATE TABLE T1";
        String expectedSql = "SHOW CREATE TABLE T1";
        assertWarning(sql, expectedSql);
    }

    @Test
    public void testShowTables() throws Exception
    {
        String sql = "SHOW TABLES FROM TEST";
        String expectedSql = "SHOW TABLES FROM TEST";
        assertSuccess(sql, expectedSql);

        sql = "SHOW TABLES FROM TEST LIKE 'HETU|TEST'";
        assertUnsupported(sql, Optional.of("LIKE"));
    }

    @Test
    public void testShowSchemas() throws Exception
    {
        String sql = "SHOW SCHEMAS";
        String expectedSql = "SHOW SCHEMAS";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testShowColumns() throws Exception
    {
        String sql = "SHOW COLUMNS FROM DB.TB1";
        String expectedSql = "SHOW COLUMNS FROM DB.TB1";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testDescribe() throws Exception
    {
        String sql1 = "DESC TEST";
        String expectedSql1 = "SHOW COLUMNS FROM TEST";
        assertSuccess(sql1, expectedSql1);

        String sql2 = "DESCRIBE DB.TB1";
        String expectedSql2 = "SHOW COLUMNS FROM DB.TB1";
        assertSuccess(sql2, expectedSql2);

        String sql3 = "DESCRIBE DEFAULT.SRC_THRIFT LINTSTRING.$ELEM$.MYINT";
        assertUnsupported(sql3, Optional.of("DESCRIBE TABLE OPTION"));

        String sql4 = "DESCRIBE EXTENDED DB.TB1";
        assertUnsupported(sql4, Optional.of("EXTENDED"));
    }

    @Test
    public void testShowRoles() throws Exception
    {
        String sql = "SHOW ROLES";
        String expectedSql = "SHOW ROLES";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testFunctions() throws Exception
    {
        String sql = "SHOW FUNCTIONS";
        String expectedSql = "SHOW FUNCTIONS";
        assertSuccess(sql, expectedSql);

        sql = "SHOW FUNCTIONS LIKE 'YEAR'";
        assertUnsupported(sql, Optional.of("LIKE"));

        sql = "SELECT IF(ID=10, 1, 2) FROM T1";
        expectedSql = "SELECT IF((ID = 10), 1, 2)\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql, expectedSql);

        sql = "SELECT NULLIF(ID1, ID2) FROM T1";
        expectedSql = "SELECT NULLIF(ID1, ID2)\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testSet() throws Exception
    {
        String sql = "SET";
        String expectedSql = "SHOW SESSION";
        assertSuccess(sql, expectedSql);

        String sql2 = "SET PATH='/USR/HIVE/TST'";
        assertUnsupported(sql2, Optional.of("SET PROPERTY"));
    }

    @Test
    public void testHiveTpcdsSql() throws Exception
    {
        String sqlFile = getClass().getClassLoader().getResource("hive-tpcds.sql").getFile();
        String query = asCharSource(new File(sqlFile), UTF_8).read();
        StatementSplitter splitter = new StatementSplitter(query);
        for (StatementSplitter.Statement split : splitter.getCompleteStatements()) {
            JSONObject result = sqlConverter.convert(split.statement());
            assertEquals(getConversionStatus(result), "Success");
            checkCanParsedByHetu(getConvertedSql(result));
        }
    }

    @Test
    public void testCreateMaterializedView()
    {
        String sql = "CREATE MATERIALIZED VIEW TEST STORED AS 'org.apache.hadoop.hive.druid.DruidStorageHandler' AS SELECT * FROM T1";
        assertUnsupported(sql, Optional.of("CREATE MATERIALIZED VIEW"));
    }

    @Test
    public void testDropMaterializedView()
    {
        String sql = "DROP MATERIALIZED VIEW TEST";
        assertUnsupported(sql, Optional.of("DROP MATERIALIZED VIEW"));
    }

    @Test
    public void testAlterMaterializedView()
    {
        String sql = "ALTER MATERIALIZED VIEW TEST ENABLE REWRITE";
        assertUnsupported(sql, Optional.of("ALTER MATERIALIZED VIEW"));
    }

    @Test
    public void testShowMaterializedViews()
    {
        String sql = "SHOW MATERIALIZED VIEWS IN DB1";
        assertUnsupported(sql, Optional.of("SHOW MATERIALIZED VIEW"));
    }

    @Test
    public void testCreateFunction()
    {
        String sql = "CREATE FUNCTION XXOO_LOWER AS 'TEST.QL.LOWERUDF' USING JAR 'HDFS:///PATH/TO/HIVE_FUNC/LOWER.JAR'";
        assertUnsupported(sql, Optional.of("CREATE FUNCTION"));
    }

    @Test
    public void testDropFunction()
    {
        String sql = "DROP FUNCTION TEST";
        assertUnsupported(sql, Optional.of("DROP FUNCTION"));
    }

    @Test
    public void testReloadFunctions()
    {
        String sql = "RELOAD FUNCTIONS";
        assertUnsupported(sql, Optional.of("RELOAD FUNCTION"));
    }

    @Test
    public void testCreateIndex()
    {
        String sql = "CREATE INDEX INDEX_STUDENTID ON TABLE STUDENT_3(STUDENTID) AS 'ORG.APACHE.HADOOP.HIVE.QL.INDEX.COMPACT.COMPACTINDEXHANDLER' WITH DEFERRED REBUILD IN TABLE INDEX_TABLE_STUDENT";
        assertUnsupported(sql, Optional.of("CREATE INDEX"));
    }

    @Test
    public void testDropIndex()
    {
        String sql = "DROP INDEX INDEX_STUDENTID ON STUDENT_3";
        assertUnsupported(sql, Optional.of("DROP INDEX"));
    }

    @Test
    public void testAlterIndex()
    {
        String sql = "ALTER INDEX INDEX_STUDENTID ON STUDENT_3 REBUILD";
        assertUnsupported(sql, Optional.of("ALTER INDEX"));
    }

    @Test
    public void testShowIndex()
    {
        String sql = "SHOW INDEX ON STUDENT_3";
        assertUnsupported(sql, Optional.of("SHOW INDEX"));
    }

    @Test
    public void testShowPartitions()
    {
        String sql = "SHOW PARTITIONS TEST_PARTITION WHERE ID > 10 LIMIT 3";
        assertUnsupported(sql, Optional.of("SHOW PARTITIONS"));
    }

    @Test
    public void testDescribePartition()
    {
        String sql = "DESCRIBE FORMATTED DEFAULT.SRC_TABLE PARTITION (PART_COL = 100) COLUMNA";
        assertUnsupported(sql, Optional.of("DESCRIBE PARTITIONS"));
    }

    @Test
    public void testCreateMacro()
    {
        String sql = "CREATE TEMPORARY MACRO STRING_LEN_PLUS_TWO(X STRING) LENGTH(X) + 2";
        assertUnsupported(sql, Optional.of("CREATE MACRO"));
    }

    @Test
    public void testDropMacro()
    {
        String sql = "DROP TEMPORARY MACRO TEST";
        assertUnsupported(sql, Optional.of("DROP MACRO"));
    }

    @Test
    public void testShowRoleGrant()
    {
        String sql = "SHOW ROLE GRANT USER TEST";
        assertUnsupported(sql, Optional.of("SHOW ROLE GRANT"));
    }

    @Test
    public void testShowPrincipals()
    {
        String sql = "SHOW PRINCIPALS TEST";
        assertUnsupported(sql, Optional.of("SHOW PRINCIPALS"));
    }

    @Test
    public void testInsertFilesystem()
    {
        String sql = "INSERT OVERWRITE DIRECTORY '/USER/HIVE/TEST' SELECT * FROM T1";
        assertUnsupported(sql, Optional.of("INSERT FILESYSTEM"));
    }

    @Test
    public void testShowLocks()
    {
        String sql = "show locks t_real_user";
        assertUnsupported(sql, Optional.of("SHOW LOCKS"));
    }

    @Test
    public void testShowConf()
    {
        String sql = "SHOW CONF 'hive.exec.parallel'";
        assertUnsupported(sql, Optional.of("SHOW CONF"));
    }

    @Test
    public void testShowTransactions()
    {
        String sql = "SHOW TRANSACTIONS";
        assertUnsupported(sql, Optional.of("SHOW TRANSACTIONS"));
    }

    @Test
    public void testShowCompactions()
    {
        String sql = "SHOW COMPACTIONS";
        assertUnsupported(sql, Optional.of("SHOW COMPACTIONS"));
    }

    @Test
    public void testAbortTransactions()
    {
        String sql = "ABORT TRANSACTIONS 0001";
        assertUnsupported(sql, Optional.of("ABORT TRANSACTIONS"));
    }

    @Test
    public void testLoadData()
    {
        String sql = "LOAD DATA LOCAL INPATH '/PATH/TO/LOCAL/FILES' OVERWRITE  INTO TABLE TEST PARTITION (COUNTRY='CHINA')";
        assertUnsupported(sql, Optional.of("LOAD DATA"));
    }

    @Test
    public void testSetSession()
    {
        String sql = "set hive.support.concurrency=true";
        assertUnsupported(sql, Optional.of("SET PROPERTY"));
    }

    @Test
    public void testResetSession()
    {
        String sql = "RESET";
        assertUnsupported(sql, Optional.of("RESET"));
    }

    @Test
    public void testMerge()
    {
        String sql = "MERGE INTO CUSTOMER AS T\n" +
                "USING ( SELECT * FROM NEW_CUSTOMER_STAGE) AS S\n" +
                "ON SUB.ID = CUSTOMER.ID\n" +
                "WHEN MATCHED THEN UPDATE SET NAME = SUB.NAME, STATE = SUB.NEW_STATE\n" +
                "WHEN NOT MATCHED THEN INSERT VALUES (SUB.ID, SUB.NAME, SUB.STATE)";
        assertUnsupported(sql, Optional.of("MERGE"));
    }

    @Test
    public void testExport()
    {
        String sql = "EXPORT TABLE DEPARTMENT TO 'HDFS_EXPORTS_LOCATION/DEPARTMENT'";
        assertUnsupported(sql, Optional.of("EXPORT TABLE"));
    }

    @Test
    public void testImport()
    {
        String sql = "IMPORT TABLE IMPORTED_DEPT FROM 'HDFS_EXPORTS_LOCATION/DEPARTMENT'";
        assertUnsupported(sql, Optional.of("IMPORT TABLE"));
    }

    @Test
    public void testDigitalIdentifer()
    {
        String sql = "select sum(b.8_0cs) from t1";
        assertUnsupported(sql, Optional.of("[DIGIT IDENTIFIER] is not supported"));
    }

    @Test
    public void testLateralView()
    {
        String sql = "SELECT C1,C2 FROM T1 LATERAL VIEW EXPLODE(COL1) MYTABLE1 AS MYCOL1 LATERAL VIEW EXPLODE(COL2) MYTABLE2 AS MYCOL2";
        assertUnsupported(sql, Optional.of("LATERAL VIEW"));
    }

    @Test
    public void testDescribeFunction()
    {
        String sql = "DESCRIBE FUNCTION GET_JSON_OBJECT";
        assertUnsupported(sql, Optional.of("Describe Function"));

        sql = "DESC FUNCTION EXTENDED GET_JSON_OBJECT";
        assertUnsupported(sql, Optional.of("Describe Function"));
    }

    @Test
    public void testBitArithmetic()
    {
        String sql = "select ~3";
        assertUnsupported(sql, Optional.of("Bit arithmetic"));

        sql = "select 2 & 3";
        assertUnsupported(sql, Optional.of("Bit arithmetic"));

        sql = "select 2 | 3";
        assertUnsupported(sql, Optional.of("Bit arithmetic"));

        sql = "select 2 ^ 3";
        assertUnsupported(sql, Optional.of("Bit arithmetic"));
    }

    @Test
    public void testNonArithmetic()
    {
        String sql = "select * from t1 where name is !null";
        String expectedSql = "SELECT *\n" +
                "FROM\n" +
                "  t1\n" +
                "WHERE (name IS NOT NULL)\n";
        assertSuccess(sql, expectedSql);

        sql = "select * from t1 where id ! between 10 and 20";
        expectedSql = "SELECT *\n" +
                "FROM\n" +
                "  t1\n" +
                "WHERE (NOT (id BETWEEN 10 AND 20))\n";
        assertSuccess(sql, expectedSql);

        sql = "select * from t1 where id ! in (10,20)";
        expectedSql = "SELECT *\n" +
                "FROM\n" +
                "  t1\n" +
                "WHERE (NOT (id IN (10, 20)))\n";
        assertSuccess(sql, expectedSql);

        sql = "select * from t1 where id1 ! in (select id2 from t2)";
        expectedSql = "SELECT *\n" +
                "FROM\n" +
                "  t1\n" +
                "WHERE (NOT (id1 IN (SELECT id2\n" +
                "FROM\n" +
                "  t2\n" +
                ")))\n";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testExpressionPredicated()
    {
        String sql = "SELECT * FROM T1 WHERE GET_JSON_OBJECT(REPORT_EVT_CONTENT, '$.PAIRRESULT')<>-1 IS NULL";
        assertUnsupported(sql, Optional.of("Unsupported statement"));

        sql = "SELECT * FROM T1 WHERE ID=AGE=20";
        assertUnsupported(sql, Optional.of("Unsupported statement"));

        sql = "SELECT * FROM T1 WHERE PT_D BETWEEN 10 AND PT_D=30 AND ID > 50";
        assertUnsupported(sql, Optional.of("Unsupported statement"));
    }

    @Test
    public void testQuery()
    {
        // test "sort by"
        String sql = "SELECT C1 FROM T1 GROUP BY NAME SORT BY CNT";
        assertUnsupported(sql, Optional.of("SORT BY"));

        // test "GROUPING SETS"
        sql = "SELECT C1 FROM T1 GROUP BY URL GROUPING SETS ((ID, NAME), (ID, SEX))";
        String expectedSql = "SELECT C1\n" +
                "FROM\n" +
                "  T1\n" +
                "GROUP BY URL, GROUPING SETS ((ID, NAME), (ID, SEX))\n";
        assertSuccess(sql, expectedSql);

        // test function "current_date()" and "current_timestamp()"
        sql = "select current_date()";
        expectedSql = "SELECT current_date\n" +
                "\n";
        assertSuccess(sql, expectedSql);

        sql = "select current_timestamp()";
        expectedSql = "SELECT current_timestamp\n" +
                "\n";
        assertSuccess(sql, expectedSql);

        // test like, rlike, regexp
        sql = "select * from t1 where name !like 'hetu'";
        expectedSql = "SELECT *\n" +
                "FROM\n" +
                "  t1\n" +
                "WHERE (NOT (name LIKE 'hetu'))\n";
        assertSuccess(sql, expectedSql);

        sql = "select * from t1 where name !rlike 'hetu'";
        assertUnsupported(sql, Optional.of("rlike"));

        sql = "select * from t1 where name !regexp 'hetu'";
        assertUnsupported(sql, Optional.of("regexp"));

        // test "=="
        sql = "select * from t1 where id == 10";
        expectedSql = "SELECT *\n" +
                "FROM\n" +
                "  t1\n" +
                "WHERE (id = 10)\n";
        assertSuccess(sql, expectedSql);

        // test "\'"
        sql = "SELECT REGEXP_REPLACE(EXT_FIELD, '[]|\\\"|\\'', '') AS CONTENT FROM ADS";
        assertUnsupported(sql, Optional.of("Unsupported string"));
    }

    @Test
    public void testExecuteCommand()
            throws IOException
    {
        String sqlFile = getClass().getClassLoader().getResource("hive-batch-test.sql").getFile();
        String query = asCharSource(new File(sqlFile), UTF_8).read();
        SessionProperties sessionProperties = new SessionProperties();
        sessionProperties.setSourceType(SqlSyntaxType.HIVE);
        sessionProperties.setConsolePrintEnable(true);
        sessionProperties.setParsingOptions(true);
        Console console = new Console();
        console.executeCommand(query, "./testExecuteCommand-hive", sessionProperties);
    }

    private String getConvertedSql(JSONObject result)
    {
        String convertedSql = "";
        try {
            convertedSql = result.get("convertedSql").toString();
        }
        catch (JSONException e) {
            fail("Get converted sql failed");
        }

        return convertedSql;
    }

    private String getConversionStatus(JSONObject result)
    {
        String convertedSql = "";
        try {
            convertedSql = result.get("status").toString();
        }
        catch (JSONException e) {
            fail("Get conversion message failed");
        }

        return convertedSql;
    }

    private void checkCanParsedByHetu(String query)
    {
        try {
            sqlParser.createStatement(query, parsingOptions).toString();
        }
        catch (Exception e) {
            fail("The converted SQL can not be parsed by Presto");
        }
    }

    private void assertFailed(String inputSql)
    {
        JSONObject result = sqlConverter.convert(inputSql);
        assertEquals(getConversionStatus(result), FAILED);
    }

    private void assertWarning(String inputSql, String expectedSql)
    {
        JSONObject result = sqlConverter.convert(inputSql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), WARNING);
        checkCanParsedByHetu(getConvertedSql(result));
    }

    private void assertSuccess(String inputSql, String expectedSql)
    {
        JSONObject result = sqlConverter.convert(inputSql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), SUCCESS);
        checkCanParsedByHetu(getConvertedSql(result));
    }

    private void assertUnsupported(String inputSql, Optional<String> unsupportedKeyWords)
    {
        JSONObject result = sqlConverter.convert(inputSql);
        assertEquals(getConversionStatus(result), UNSUPPORTED);
        try {
            String msg = result.get(MESSAGE).toString();
            if (unsupportedKeyWords.isPresent()) {
                assertTrue(msg.toLowerCase(Locale.ENGLISH).contains(unsupportedKeyWords.get().toLowerCase(Locale.ENGLISH)));
            }
        }
        catch (JSONException e) {
            fail("Get converted sql failed");
        }
    }
}
