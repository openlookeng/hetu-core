/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

import io.hetu.core.sql.migration.Constants;
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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestImpalaSqlMigrate
{
    private SqlParser sqlParser;
    private ParsingOptions parsingOptions;
    private SqlSyntaxConverter sqlConverter;

    @BeforeClass
    public void setup() throws Exception
    {
        sqlParser = new SqlParser();
        parsingOptions = new ParsingOptions(DecimalLiteralTreatment.AS_DECIMAL);
        SessionProperties sessionProperties = new SessionProperties();
        sessionProperties.setSourceType(SqlSyntaxType.IMPALA);
        sessionProperties.setParsingOptions(false);
        sqlConverter = SqlConverterFactory.getSqlConverter(sessionProperties);
    }

    @Test
    public void testUseStatement()
    {
        String sql = "use abc";
        String expectedSql = "USE abc";

        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testCreateSchema()
    {
        String sql = "CREATE SCHEMA IF NOT EXISTS S1 COMMENT 'hetu' LOCATION '/user'";
        String sql2 = "CREATE DATABASE IF NOT EXISTS S1 LOCATION '/user'";

        String expectedSql = "CREATE SCHEMA IF NOT EXISTS S1\n" +
                "WITH (\n" +
                "   location = '/user'\n" +
                ")";
        assertWarning(sql, expectedSql);
        assertSuccess(sql2, expectedSql);
    }

    @Test
    public void testDropSchema()
    {
        String sql1 = "drop schema S1 RESTRICT";
        String sql2 = "drop database S1";
        String expectedSql = "DROP SCHEMA S1 RESTRICT";
        assertSuccess(sql1, expectedSql);
        assertSuccess(sql2, expectedSql);

        // CASECADE keyword is not support
        String sql3 = "drop schema S1 CASCADE";
        assertUnsupported(sql3, Optional.of("CASCADE"));
    }

    @Test
    public void testAlterSchema()
    {
        String sql1 = "ALTER DATABASE db1 SET OWNER USER tester";
        String sql2 = "ALTER DATABASE db1 SET OWNER ROLE tester";
        assertUnsupported(sql1, Optional.of("ALTER DATABASE"));
        assertUnsupported(sql2, Optional.of("ALTER DATABASE"));
    }

    @Test
    public void testCreateTable()
    {
        String sql = "CREATE TABLE tb1 (id INT, name STRING)";
        String expectedSql = "CREATE TABLE tb1 (\n" +
                "   id INT,\n" +
                "   name STRING\n" +
                ")";
        assertSuccess(sql, expectedSql);

        String sqlExternal = "CREATE EXTERNAL TABLE tb1 (id INT, name STRING) LOCATION '/user/tmp'";
        String expectedSqlExternal = "CREATE TABLE tb1 (\n" +
                "   id INT,\n" +
                "   name STRING\n" +
                ")\n" +
                "WITH (\n" +
                "   external = true,\n" +
                "   location = '/user/tmp'\n" +
                ")";
        assertSuccess(sqlExternal, expectedSqlExternal);

        // Test positive properties
        String sql2 = "CREATE TABLE tbl_partitioned (id INT, name STRING) PARTITIONED BY (p1 INT, p2 STRING) " +
                "COMMENT 'this is a comment' STORED AS PARQUET LOCATION '/user/tmp'";
        String expectedSql2 = "CREATE TABLE tbl_partitioned (\n" +
                "   id INT,\n" +
                "   name STRING,\n" +
                "   p1 INT,\n" +
                "   p2 STRING\n" +
                ")\n" +
                "COMMENT 'this is a comment'\n" +
                "WITH (\n" +
                "   partitioned_by = ARRAY['p1','p2'],\n" +
                "   format = 'Parquet',\n" +
                "   location = '/user/tmp'\n" +
                ")";
        assertSuccess(sql2, expectedSql2);

        String sql3 = "create table impala10(impala1 tinyint, impala2 smallint, impala3 int) TBLPROPERTIES('id'='11','cnt'=1)";
        assertUnsupported(sql3, Optional.of("TBLPROPERTIES"));

        String sql4 = "create table impala10(impala1 tinyint, impala2 smallint, impala3 int) sort by (impala3)";
        String expectedSql4 = "CREATE TABLE impala10 (\n" +
                "   impala1 tinyint,\n" +
                "   impala2 smallint,\n" +
                "   impala3 int\n" +
                ")\n" +
                "WITH (\n" +
                "   sorted_by = ARRAY['impala3']\n" +
                ")";
        assertSuccess(sql4, expectedSql4);

        String sql5 = "create table impala10(impala1 tinyint, impala2 smallint, impala3 int) tblproperties('transactional'='true')";
        String expectedSql5 = "CREATE TABLE impala10 (\n" +
                "   impala1 tinyint,\n" +
                "   impala2 smallint,\n" +
                "   impala3 int\n" +
                ")\n" +
                "WITH (\n" +
                "   transactional = true\n" +
                ")";
        assertSuccess(sql5, expectedSql5);

        // Test negative sql - row format is not supported
        String negativeSql1 = "CREATE TABLE tbl_row_format (id INT, name STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY 'char'";
        assertUnsupported(negativeSql1, Optional.of("ROW"));

        // WITH SERDEPROPERTIES is not supported
        String negativeSql2 = "CREATE TABLE tbl_row_format (id INT, name STRING) WITH SERDEPROPERTIES ('key'='value')";
        assertUnsupported(negativeSql2, Optional.of("SERDEPROPERTIES"));

        // CACHED IN is not supported
        String negativeSql3 = "CREATE TABLE tbl_row_format (id INT, name STRING) CACHED IN 'cache_pool_name' WITH REPLICATION = 2";
        assertUnsupported(negativeSql3, Optional.of("CACHED"));

        String negativeSql4 = "create table impala10(impala1 tinyint, impala2 smallint, impala3 int) tblproperties('debug'='true')";
        assertUnsupported(negativeSql4, Optional.of("TBLPROPERTIES"));
    }

    @Test
    public void testCreateTableTypeDefinition()
    {
        String sql1 = "CREATE TABLE tb1 (c1 TINYINT, c2 SMALLINT, c3 INT, c4 BIGINT, c5 Boolean, c6 CHAR(10), c7 VARCHAR(10), " +
                "c8 STRING, c9 DATE, c10 TIMESTAMP, c11 DECIMAL(10,3), c12 DOUBLE, c13 REAL, c14 FLOAT)";
        String expectedSql1 = "CREATE TABLE tb1 (\n" +
                "   c1 TINYINT,\n" +
                "   c2 SMALLINT,\n" +
                "   c3 INT,\n" +
                "   c4 BIGINT,\n" +
                "   c5 Boolean,\n" +
                "   c6 CHAR(10),\n" +
                "   c7 VARCHAR(10),\n" +
                "   c8 STRING,\n" +
                "   c9 DATE,\n" +
                "   c10 TIMESTAMP,\n" +
                "   c11 DECIMAL(10,3),\n" +
                "   c12 DOUBLE,\n" +
                "   c13 REAL,\n" +
                "   c14 FLOAT\n" +
                ")";
        assertSuccess(sql1, expectedSql1);

        String sql2 = "CREATE TABLE tb2 (c1 ARRAY<INT>, c2 MAP <STRING, ARRAY <STRING>>)";
        String expectedSql2 = "CREATE TABLE tb2 (\n" +
                "   c1 ARRAY(INT),\n" +
                "   c2 MAP(STRING,ARRAY(STRING))\n" +
                ")";
        assertSuccess(sql2, expectedSql2);

        String negativeSql3 = "CREATE TABLE tb3 (c1 STRUCT <employer:STRING, id : BIGINT, address : STRING>)";
        assertUnsupported(negativeSql3, Optional.of("STRUCT"));
    }

    @Test
    public void testCreateTableAs()
    {
        String sql = "CREATE TABLE tb1 AS SELECT * FROM tb2";
        String expectedSql = "CREATE TABLE tb1 AS SELECT *\n" +
                "FROM\n" +
                "  tb2\n";

        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testCreateTableLike()
    {
        String sql = "CREATE EXTERNAL TABLE T2 LIKE T1 COMMENT 'HETU' STORED AS AVRO LOCATION '/USER/TEST'";
        String expectedSql = "CREATE TABLE T2 (\n" +
                "   LIKE T1 INCLUDING PROPERTIES\n" +
                ")\n" +
                "COMMENT 'HETU'\n" +
                "WITH (\n" +
                "   external = true,\n" +
                "   location = '/USER/TEST',\n" +
                "   format = 'Avro'\n" +
                ")";
        assertSuccess(sql, expectedSql);

        String sql2 = "CREATE EXTERNAL TABLE T2 LIKE PARQUET '/user/test/impala' COMMENT 'HETU' STORED AS AVRO LOCATION '/USER/TEST'";
        assertUnsupported(sql2, Optional.of("PARQUET"));
    }

    @Test
    public void testCreateKuduTable()
    {
        String sql = "CREATE TABLE KUDU_T3 (ID BIGINT, YEAR INT, S STRING,\n" +
                "    B BOOLEAN, PRIMARY KEY (ID,YEAR))\n" +
                "  PARTITION BY HASH (ID) PARTITIONS 20,\n" +
                "  RANGE (YEAR) (PARTITION 1980 <= VALUES < 1990,\n" +
                "    PARTITION 1990 <= VALUES < 2000,\n" +
                "    PARTITION VALUE = 2001,\n" +
                "    PARTITION 2001 < VALUES)\n" +
                "  STORED AS KUDU";
        assertUnsupported(sql, Optional.of("Create Kudu Table"));
    }

    @Test
    public void testCreateKuduTableAsSelect()
    {
        String sql = "CREATE TABLE KUDU_T3 (PRIMARY KEY (ID,YEAR))\n" +
                "  PARTITION BY HASH (ID) PARTITIONS 20,\n" +
                "  RANGE (YEAR) (PARTITION 1980 <= VALUES < 1990,\n" +
                "    PARTITION 1990 <= VALUES < 2000,\n" +
                "    PARTITION VALUE = 2001,\n" +
                "    PARTITION 2001 < VALUES)\n" +
                "  STORED AS KUDU AS SELECT ID, YEAR, MONTH FROM T1";
        assertUnsupported(sql, Optional.of("Create Kudu Table"));
    }

    @Test
    public void testSelectNoWith()
    {
        // complicated select will be verified in by testImpalaTpcdsSql()
        String sql1 = "SELECT * FROM tb1 WHERE c1='a' AND c2=1 AND c3>0 AND c4<10 AND c5<>0 LIMIT 100";
        String expectedSql1 = "SELECT *\n" +
                "FROM\n" +
                "  tb1\n" +
                "WHERE (((((c1 = 'a') AND (c2 = 1)) AND (c3 > 0)) AND (c4 < 10)) AND (c5 <> 0))\n" +
                "LIMIT 100\n";
        assertSuccess(sql1, expectedSql1);

        // test distinct key words and limit, order by, and offset keyword
        String sql2 = "SELECT DISTINCT c1, c2, c3 FROM tb1 WHERE c1=1 ORDER BY c2 LIMIT 5 OFFSET 1";
        String expectedSql2 = "SELECT DISTINCT\n" +
                "  c1\n" +
                ", c2\n" +
                ", c3\n" +
                "FROM\n" +
                "  tb1\n" +
                "WHERE (c1 = 1)\n" +
                "ORDER BY c2 ASC\n" +
                "OFFSET 1 ROWS\n" +
                "LIMIT 5\n";
        assertSuccess(sql2, expectedSql2);

        // grouping by
        String sql3 = "SELECT COUNT(c1) AS a1 FROM tb1 GROUP BY c1 ORDER BY c1 DESC LIMIT 5";
        String expectedSql3 = "SELECT \"count\"(c1) a1\n" +
                "FROM\n" +
                "  tb1\n" +
                "GROUP BY c1\n" +
                "ORDER BY c1 DESC\n" +
                "LIMIT 5\n";
        assertSuccess(sql3, expectedSql3);

        // having clasue
        String sql4 = "SELECT COUNT(name) AS cnt FROM EMPLOYEE GROUP BY name HAVING MAX(salary)>110 LIMIT 5";
        String expectedSql4 = "SELECT \"count\"(name) cnt\n" +
                "FROM\n" +
                "  EMPLOYEE\n" +
                "GROUP BY name\n" +
                "HAVING (\"max\"(salary) > 110)\n" +
                "LIMIT 5\n";
        assertSuccess(sql4, expectedSql4);

        // teet join
        String sql5 = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id";
        String expectedSql5 = "SELECT *\n" +
                "FROM\n" +
                "  (t1\n" +
                "INNER JOIN t2 ON (t1.id = t2.id))\n";
        assertSuccess(sql5, expectedSql5);

        String sql6 = "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id";
        assertSuccess(sql6, expectedSql5);

        String sql7 = "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id";
        String expectedSql7 = "SELECT *\n" +
                "FROM\n" +
                "  (t1\n" +
                "FULL JOIN t2 ON (t1.id = t2.id))\n";
        assertSuccess(sql7, expectedSql7);

        String sql8 = "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id";
        String expectedSql8 = "SELECT *\n" +
                "FROM\n" +
                "  (t1\n" +
                "LEFT JOIN t2 ON (t1.id = t2.id))\n";
        assertSuccess(sql8, expectedSql8);

        String sql9 = "SELECT * FROM t1 CROSS JOIN t2 WHERE t1.id > t2.id";
        String expectedSql9 = "SELECT *\n" +
                "FROM\n" +
                "  (t1\n" +
                "CROSS JOIN t2)\n" +
                "WHERE (t1.id > t2.id)\n";
        assertSuccess(sql9, expectedSql9);

        String sql10 = "SELECT * FROM t1 where id1 is distinct from id2";
        String expectedSql10 = "SELECT *\n" +
                "FROM\n" +
                "  t1\n" +
                "WHERE (id1 IS DISTINCT FROM id2)\n";
        assertSuccess(sql10, expectedSql10);

        String sql11 = "SELECT * FROM t1 where id1 is not distinct from id2";
        String expectedSql11 = "SELECT *\n" +
                "FROM\n" +
                "  t1\n" +
                "WHERE (NOT (id1 IS DISTINCT FROM id2))\n";
        assertSuccess(sql11, expectedSql11);

        String sql12 = "SELECT * FROM t1 where id1 > -id2";
        String expectedSql12 = "SELECT *\n" +
                "FROM\n" +
                "  t1\n" +
                "WHERE (id1 > -(id2))\n";
        assertSuccess(sql12, expectedSql12);

        String sql13 = "SELECT CURRENT_TIMESTAMP()";
        String expectedSql13 = "SELECT current_timestamp\n" +
                "\n";
        assertSuccess(sql13, expectedSql13);
    }

    @Test
    public void testRenameTable()
    {
        String sql1 = "ALTER TABLE t1 RENAME TO t2";
        String expectedSql = "ALTER TABLE t1 RENAME TO t2";
        assertSuccess(sql1, expectedSql);
    }

    @Test
    public void testDropColumn()
    {
        String sql = "ALTER TABLE t1 DROP COLUMN c1";
        String expectedSql = "ALTER TABLE t1 DROP COLUMN c1";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testAlterColumn()
    {
        // add kudu column
        String sql1 = "ALTER TABLE t1 ADD COLUMN IF NOT EXISTS n1 INT COMMENT 'NEW 1'";
        String sql2 = "ALTER TABLE t1 ADD COLUMN n1 INT COMMENT 'NEW 1'";
        String expectedSql2 = "ALTER TABLE t1 ADD COLUMN n1 INT COMMENT 'NEW 1'";
        // using add columns but only passed on column, it will be equal to add single column
        String sql3 = "ALTER TABLE t1 ADD COLUMNS (n1 INT) COMMENT 'NEW 1'";
        String sql4 = "ALTER TABLE t1 ADD COLUMNS (n1 INT COMMENT 'NEW 1')";
        assertUnsupported(sql1, Optional.of("EXISTS"));

        assertSuccess(sql2, expectedSql2);
        assertFailed(sql3);
        assertSuccess(sql4, sql2);

        String sql5 = "ALTER TABLE t1 ADD IF NOT EXISTS COLUMNS (n1 INT)";
        assertUnsupported(sql5, Optional.of("IF NOT EXISTS"));
    }

    @Test
    public void testNegativeAlterTable()
    {
        // add columns
        String sql1 = "ALTER TABLE T1 ADD COLUMNS (N1 INT COMMENT 'NEW 1', N2 INT COMMENT 'NEW 2')";
        assertUnsupported(sql1, Optional.of("Multiple columns"));

        // replace column
        String sql2 = "ALTER TABLE t1 REPLACE COLUMNS (n1 INT COMMENT 'NEW 1', n2 INT COMMENT 'NEW 1')";
        assertUnsupported(sql2, Optional.of("Replace Column"));

        // set owner
        String sql4 = "ALTER TABLE t1 SET OWNER USER test";
        assertUnsupported(sql4, Optional.of("Alter Owner"));

        // alter kudu table column
        String sql5 = "ALTER TABLE t1 ALTER c1 { SET BLOCK_SIZE 3}";
        assertUnsupported(sql5, Optional.of("Alter Kudu table"));
    }

    @Test
    public void testDropTable()
    {
        String sql1 = "DROP TABLE t1";
        assertSuccess(sql1, sql1);

        String sql2 = "DROP TABLE IF EXISTS t1";
        assertSuccess(sql2, sql2);

        String sql3 = "DROP TABLE t1 PURGE";
        assertUnsupported(sql3, Optional.of("PURGE"));
    }

    @Test
    public void testTruncateTable()
    {
        assertUnsupported("TRUNCATE TABLE tb1", Optional.of("Truncate"));
    }

    @Test
    public void testCreateView()
    {
        String sql1 = "CREATE VIEW v1 AS SELECT * FROM t1";
        String expectedSql1 = "CREATE VIEW v1 AS\n" +
                "SELECT *\n" +
                "FROM\n" +
                "  t1\n";
        assertSuccess(sql1, expectedSql1);

        String sql2 = "CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM t1";
        assertUnsupported(sql2, Optional.of("EXISTS"));

        String sql3 = "CREATE VIEW v1 (c1, c2) AS SELECT * FROM t1";
        assertUnsupported(sql3, Optional.of("COLUMN ALIASES"));
    }

    @Test
    public void testAlterView()
    {
        String sql1 = "ALTER VIEW v1 AS SELECT c1, c2 FROM t2";
        String expectedSql1 = "CREATE OR REPLACE VIEW v1 AS\n" +
                "SELECT\n" +
                "  c1\n" +
                ", c2\n" +
                "FROM\n" +
                "  t2\n";
        assertSuccess(sql1, expectedSql1);

        String sql2 = "ALTER VIEW v1 (n1, n2) AS SELECT c1, c2 FROM t2";
        assertUnsupported(sql2, Optional.of("COLUMN ALIASES"));
    }

    @Test
    public void testRenameView()
    {
        String sql1 = "ALTER VIEW v1 RENAME TO v2";
        assertUnsupported(sql1, Optional.of("rename"));
    }

    @Test
    public void testAlterViewOwner()
    {
        String sql1 = "ALTER VIEW v1 SET OWNER USER test";
        assertUnsupported(sql1, Optional.of("OWNER"));
    }

    @Test
    public void testDropView()
    {
        String sql1 = "DROP VIEW IF EXISTS v1";
        assertSuccess(sql1, sql1);
    }

    @Test
    public void testDescribeDbOrTable()
    {
        String sql1 = "DESCRIBE DATABASE db1";
        assertUnsupported(sql1, Optional.of("DATABASE"));

        String sql2 = "DESCRIBE FORMATTED t1";
        assertUnsupported(sql2, Optional.of("FORMATTED"));

        String sql3 = "DESCRIBE EXTENDED t1";
        assertUnsupported(sql3, Optional.of("EXTENDED"));

        String sql4 = "DESCRIBE t1";
        assertSuccess(sql4, "SHOW COLUMNS FROM t1");
    }

    @Test
    public void testComputeStats()
    {
        assertUnsupported("COMPUTE STATS st_name", Optional.of("COMPUTE STATS"));
    }

    @Test
    public void testComputeIncrementalStats()
    {
        assertUnsupported("COMPUTE INCREMENTAL STATS st_name", Optional.of("COMPUTE INCREMENTAL STATS"));
    }

    @Test
    public void testDropStats()
    {
        assertUnsupported("DROP STATS st_name", Optional.of("DROP STATS"));
    }

    @Test
    public void testDropIncrementalStats()
    {
        assertUnsupported("DROP INCREMENTAL STATS ST_NAME PARTITION (PK='ABC')", Optional.of("DROP INCREMENTAL STATS"));
    }

    @Test
    public void testCreateFunction()
    {
        assertUnsupported("CREATE FUNCTION BIGINT_TO_TIMECONV(BIGINT) " +
                        "RETURNS TIMESTAMP LOCATION '/UDF/TIMECONVERTER_UDF.SO' SYMBOL='TIMECONVERTER'",
                Optional.of("CREATE FUNCTION"));

        assertUnsupported("CREATE FUNCTION TESTUDF " +
                        "LOCATION '/USER/IMPALA/UDFS/UDF-EXAMPLES.JAR' SYMBOL='ORG.APACHE.IMPALA.TESTUDF'",
                Optional.of("CREATE FUNCTION"));
    }

    @Test
    public void testRefreshFunction()
    {
        assertUnsupported("REFRESH FUNCTIONS db_name",
                Optional.of("REFRESH FUNCTIONS"));
    }

    @Test
    public void testDropFunction()
    {
        assertUnsupported("DROP FUNCTION fn_name",
                Optional.of("DROP FUNCTION"));
    }

    @Test
    public void testCreateRole()
    {
        String sql = "CREATE ROLE ROLE_NAME";
        assertSuccess(sql, sql);
    }

    @Test
    public void testDropRole()
    {
        String sql = "DROP ROLE ROLE_NAME";
        assertSuccess(sql, sql);
    }

    @Test
    public void testGrantRoleToGroup()
    {
        String sql = "GRANT ROLE role_name TO GROUP group_name";
        assertUnsupported(sql, Optional.of("GRANT ROLE"));
    }

    @Test
    public void testGrant()
    {
        String sql = "GRANT SELECT ON TABLE table_name TO ROLE role_name WITH GRANT OPTION";
        assertSuccess(sql, sql);

        String sql2 = "GRANT INSERT ON TABLE table_name TO ROLE role_name WITH GRANT OPTION";
        assertSuccess(sql2, sql2);

        String sql3 = "GRANT ALL ON TABLE table_name TO ROLE role_name WITH GRANT OPTION";
        assertUnsupported(sql3, Optional.of("GRANT"));

        String sql4 = "GRANT CREATE ON TABLE table_name TO ROLE role_name WITH GRANT OPTION";
        assertUnsupported(sql4, Optional.of("CREATE"));

        String sql5 = "GRANT SELECT ON DATABASE db_name TO ROLE role_name WITH GRANT OPTION";
        assertUnsupported(sql5, Optional.of("GRANT"));

        String sql6 = "GRANT SELECT ON SERVER s_name TO ROLE role_name WITH GRANT OPTION";
        assertUnsupported(sql6, Optional.of("GRANT"));

        String sql7 = "GRANT REFRESH ON SERVER s_name TO ROLE role_name WITH GRANT OPTION";
        assertUnsupported(sql7, Optional.of("REFRESH"));

        String sql8 = "GRANT SELECT(ID) ON SERVER s_name TO ROLE role_name WITH GRANT OPTION";
        assertUnsupported(sql8, Optional.of("SELECT(column_name)"));
    }

    @Test
    public void testRevokeRoleToGroup()
    {
        String sql = "REVOKE ROLE role_name FROM GROUP group_name";
        assertUnsupported(sql, Optional.of("REVOKE ROLE"));
    }

    @Test
    public void testRevoke()
    {
        String sql = "REVOKE GRANT OPTION FOR SELECT ON TABLE table_name FROM ROLE role_name";
        assertSuccess(sql, sql);

        String sql2 = "REVOKE GRANT OPTION FOR INSERT ON TABLE table_name FROM ROLE role_name";
        assertSuccess(sql2, sql2);

        String sql3 = "REVOKE ALL ON TABLE table_name FROM ROLE role_name";
        assertUnsupported(sql3, Optional.of("REVOKE"));

        String sql4 = "REVOKE CREATE ON TABLE table_name FROM ROLE role_name";
        assertUnsupported(sql4, Optional.of("CREATE"));

        String sql5 = "REVOKE SELECT ON DATABASE db_name FROM ROLE role_name";
        assertUnsupported(sql5, Optional.of("REVOKE"));

        String sql6 = "REVOKE REFRESH ON DATABASE db_name FROM ROLE role_name";
        assertUnsupported(sql6, Optional.of("REFRESH"));

        String sql7 = "REVOKE SELECT(ID) ON DATABASE db_name FROM ROLE role_name";
        assertUnsupported(sql7, Optional.of("SELECT(column_name)"));
    }

    @Test
    public void testShowTablesStats()
    {
        String sql = "SHOW TABLE STATS TEST";
        assertUnsupported(sql, Optional.of("SHOW TABLE STATS"));
    }

    @Test
    public void testShowColumnStats()
    {
        String sql = "SHOW COLUMN STATS TEST";
        String expectedSql1 = "SHOW STATS FOR TEST";
        assertSuccess(sql, expectedSql1);
    }

    @Test
    public void testShowPartitions()
    {
        String sql = "SHOW PARTITIONS TEST";
        assertUnsupported(sql, Optional.of("SHOW PARTITIONS"));
    }

    @Test
    public void testShowFiles()
    {
        String sql = "SHOW FILES IN TEST";
        assertUnsupported(sql, Optional.of("SHOW FILES"));
    }

    @Test
    public void testShowRoleGrant()
    {
        String sql = "SHOW ROLE GRANT GROUP TEST";
        assertUnsupported(sql, Optional.of("GROUP"));
    }

    @Test
    public void testShowGrantRole()
    {
        String sql = "SHOW GRANT ROLE R1";
        assertUnsupported(sql, Optional.of("GRANT ROLE"));
    }

    @Test
    public void testShowGrantUser()
    {
        String sql = "SHOW GRANT USER TEST";
        assertUnsupported(sql, Optional.of("GRANT USER"));
    }

    @Test
    public void testComment()
    {
        String sql1 = "COMMENT ON TABLE TEST IS 'HETU'";
        String expectedSql1 = "COMMENT ON TABLE test IS 'HETU'";
        assertSuccess(sql1, expectedSql1);

        String sql2 = "COMMENT ON TABLE TEST IS NULL";
        String expectedSql2 = "COMMENT ON TABLE test IS NULL";
        assertSuccess(sql2, expectedSql2);

        String sql3 = "COMMENT ON DATABASE TEST IS NULL";
        assertUnsupported(sql3, Optional.of("DATABASE/COLUMN"));
    }

    @Test
    public void testExplain()
    {
        String sql = "EXPLAIN SELECT * FROM T1";
        String expectedSql = "EXPLAIN \n" +
                "SELECT *\n" +
                "FROM\n" +
                "  T1\n";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testSet()
    {
        String sql1 = "SET MEM_LIMIT=10";
        assertUnsupported(sql1, Optional.of("IDENTIFIER"));

        String sql2 = "SET";
        String sql3 = "SET ALL";
        String expectedSql = "SHOW SESSION";
        assertSuccess(sql2, expectedSql);
        assertSuccess(sql3, expectedSql);
    }

    @Test
    public void testShutDown()
    {
        String sql = ":SHUTDOWN('LOCALHOST' : 8090, 0)";
        String sql2 = ":SHUTDOWN(\\\"hostname:1234\\\")";
        assertUnsupported(sql, Optional.of("SHUTDOWN"));
        assertUnsupported(sql2, Optional.of("SHUTDOWN"));
    }

    @Test
    public void testInvalidateMetadata()
    {
        String sql = "INVALIDATE METADATA TEST";
        assertUnsupported(sql, Optional.of("INVALIDATE METADATA"));
    }

    @Test
    public void testLoadData()
    {
        String sql = "LOAD DATA INPATH '/USER/HIVE' INTO TABLE TEST PARTITION(ID=10, NAME='test')";
        assertUnsupported(sql, Optional.of("LOAD DATA"));
    }

    @Test
    public void testRefreshMeta()
    {
        String sql = "REFRESH TEST PARTITION(ID=3, NAME='hetu')";
        assertUnsupported(sql, Optional.of("REFRESH"));
    }

    @Test
    public void testRefreshAuth()
    {
        String sql = "REFRESH AUTHORIZATION";
        assertUnsupported(sql, Optional.of("REFRESH AUTHORIZATION"));
    }

    @Test
    public void testInsert()
    {
        String sql = "INSERT INTO tb1 VALUES (1,1)";
        String expectedSql = "INSERT INTO tb1\n" +
                " VALUES \n" +
                "  ROW (1, 1)\n";
        assertSuccess(sql, expectedSql);

        String sql2 = "insert into employee (ID,NAME,AGE,ADDRESS,SALARY) VALUES (1, 'Ramesh',32, 'Ahmedabad', 20000 )";
        String expectedSql2 = "INSERT INTO employee (ID, NAME, AGE, ADDRESS, SALARY)\n" +
                " VALUES \n" +
                "  ROW (1, 'Ramesh', 32, 'Ahmedabad', 20000)\n";
        assertSuccess(sql2, expectedSql2);

        String sql3 = "insert into employee (ID,NAME,AGE,ADDRESS,SALARY) SELECT * FROM tcopy WHERE ID>1000";
        String expectedSql3 = "INSERT INTO employee (ID, NAME, AGE, ADDRESS, SALARY)\n" +
                "SELECT *\n" +
                "FROM\n" +
                "  tcopy\n" +
                "WHERE (ID > 1000)\n";
        assertSuccess(sql3, expectedSql3);

        // test with
        String sql4 = "with t1 as (select * from customers where age>25) insert into t2 select * from t1";
        assertUnsupported(sql4, Optional.of("WITH"));

        // test hint
        String sql5 = "insert /* +SHUFFLE */ into tb1 values (1,1)";
        assertUnsupported(sql5, Optional.of("hint"));

        String sql6 = "insert into tb1 /* +SHUFFLE */ values (1,1)";
        assertUnsupported(sql6, Optional.of("hint"));

        // test partition
        String sql7 = "insert into tb1 (id, name) partition (pk='v1', pk2='v2') values (1,1)";
        assertUnsupported(sql7, Optional.of("PARTITION"));
    }

    @Test
    public void testInsertOverWrite()
    {
        String sql = "INSERT OVERWRITE tb1 VALUES (1,1)";
        String expectedSql = "INSERT OVERWRITE tb1\n" +
                " VALUES \n" +
                "  ROW (1, 1)\n";
        assertSuccess(sql, expectedSql);
    }

    @Test
    public void testDelete()
    {
        String sql = "DELETE FROM TB1 WHERE ID>1";
        String expectedSql = "DELETE FROM TB1 WHERE (ID > 1)";
        assertSuccess(sql, expectedSql);

        String sql2 = "DELETE t1 FROM t1 JOIN t2 ON t1.x = t2.x";
        assertUnsupported(sql2, Optional.of("JOIN"));

        String sql3 = "DELETE T1 FROM T1 JOIN T2 ON T1.X = T2.X WHERE T1.Y = FALSE AND T2.Z > 100";
        assertUnsupported(sql3, Optional.of("JOIN"));
    }

    @Test
    public void testUpdate()
    {
        String sql = "UPDATE kudu_table SET c3 = NULL WHERE c1 > 100 AND c3 IS NULL";
        String expectedSql = "UPDATE kudu_table SET\n" +
                "c3=null\n" +
                "WHERE ((c1 > 100) AND (c3 IS NULL))\n";
        assertWarning(sql, expectedSql);

        String sql2 = "UPDATE T1 SET C3 = UPPER(C3) FROM K1 A1 JOIN K2 A2 ON A1.ID = A2.ID";
        assertUnsupported(sql2, Optional.of("relation"));
    }

    @Test
    public void testUpsert()
    {
        String sql = "UPSERT INTO kudu_table (pk, c1, c2, c3) VALUES (0, 'hello', 50, true), (1, 'world', -1, false)";
        String sql2 = "UPSERT INTO production_table SELECT * FROM staging_table";
        String sql3 = "UPSERT INTO production_table SELECT * FROM staging_table WHERE c1 IS NOT NULL AND c2 > 0";
        assertUnsupported(sql, Optional.of("UPSERT"));
        assertUnsupported(sql2, Optional.of("UPSERT"));
        assertUnsupported(sql3, Optional.of("UPSERT"));
    }

    @Test
    public void testShowSchema()
    {
        String sql = "SHOW SCHEMAS";
        String sql2 = "SHOW DATABASES";
        String sql3 = "SHOW SCHEMAS LIKE 't*'";
        String sql4 = "Show schemas like 't*'|'a*'";

        assertSuccess(sql, sql);
        assertSuccess(sql2, "SHOW SCHEMAS");
        assertSuccess(sql3, "SHOW SCHEMAS LIKE 't%'");
        assertUnsupported(sql4, Optional.of("LIKE"));
    }

    @Test
    public void testShowTables()
    {
        String sql = "SHOW TABLES IN DB1";
        String sql2 = "SHOW TABLES";
        String sql3 = "SHOW TABLES FROM DB1 LIKE 't*'";
        String sql4 = "SHOW TABLES FROM DB1 LIKE 't*'|'a*'";

        assertSuccess(sql, "SHOW TABLES FROM DB1");
        assertSuccess(sql2, sql2);
        assertSuccess(sql3, "SHOW TABLES FROM DB1 LIKE 't%'");
        assertUnsupported(sql4, Optional.of("LIKE"));
    }

    @Test
    public void testShowFunctions()
    {
        String sql = "SHOW FUNCTIONS IN DB1";
        String sql2 = "SHOW AGGREGATE FUNCTIONS IN DB1";
        String sql3 = "SHOW ANALYTIC FUNCTIONS IN DB1";
        String sql4 = "SHOW FUNCTIONS";
        String sql5 = "SHOW FUNCTIONS LIKE 'TEST'";
        assertUnsupported(sql, Optional.of("IN"));
        assertUnsupported(sql2, Optional.of("AGGREGATE"));
        assertUnsupported(sql3, Optional.of("ANALYTIC"));
        assertSuccess(sql4, "SHOW FUNCTIONS");
        assertSuccess(sql5, "SHOW FUNCTIONS LIKE 'TEST'");
    }

    @Test void testFunctionCall()
    {
        String sql = "SELECT IF(ID=10, 1, 2) FROM T1";
        String expectedSql = "SELECT IF((ID = 10), 1, 2)\n" +
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
    public void testShowRoles()
    {
        String sql = "SHOW ROLES";
        assertSuccess(sql, "SHOW ROLES");
    }

    @Test
    public void testShowCreateTableView()
    {
        String sql = "SHOW CREATE TABLE TB1";
        String sql2 = "SHOW CREATE VIEW TV1";
        assertSuccess(sql, sql);
        assertSuccess(sql2, sql2);
    }

    @Test
    public void testImpalaTpcdsSql() throws Exception
    {
        String sqlFile = getClass().getClassLoader().getResource("impala-tpcds-queries.sql").getFile();
        String query = asCharSource(new File(sqlFile), UTF_8).read();
        StatementSplitter splitter = new StatementSplitter(query);
        for (StatementSplitter.Statement split : splitter.getCompleteStatements()) {
            JSONObject result = sqlConverter.convert(split.statement());
            assertEquals(getConversionStatus(result), Constants.SUCCESS);
            checkCanParsedByHetu(getConvertedSql(result));
        }
    }

    @Test
    public void testExecuteCommand()
            throws IOException
    {
        String sqlFile = getClass().getClassLoader().getResource("impala-batch-test.sql").getFile();
        String query = asCharSource(new File(sqlFile), UTF_8).read();
        SessionProperties sessionProperties = new SessionProperties();
        sessionProperties.setSourceType(SqlSyntaxType.IMPALA);
        sessionProperties.setConsolePrintEnable(true);
        sessionProperties.setParsingOptions(true);
        Console console = new Console();
        console.executeCommand(query, "./testExecuteCommand", sessionProperties);
    }

    private void assertSuccess(String inputSql, String expectedSql)
    {
        JSONObject result = sqlConverter.convert(inputSql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), Constants.SUCCESS);
        checkCanParsedByHetu(getConvertedSql(result));
    }

    private void assertFailed(String inputSql)
    {
        JSONObject result = sqlConverter.convert(inputSql);
        assertEquals(getConversionStatus(result), Constants.FAILED);
    }

    private void assertWarning(String inputSql, String expectedSql)
    {
        JSONObject result = sqlConverter.convert(inputSql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), Constants.WARNING);
        checkCanParsedByHetu(getConvertedSql(result));
    }

    private void assertUnsupported(String inputSql, Optional<String> unsupportedKeyWords)
    {
        JSONObject result = sqlConverter.convert(inputSql);
        assertEquals(getConversionStatus(result), Constants.UNSUPPORTED);
        try {
            String msg = result.get(Constants.MESSAGE).toString();
            if (unsupportedKeyWords.isPresent()) {
                assertTrue(msg.toLowerCase(Locale.ENGLISH).contains(unsupportedKeyWords.get().toLowerCase(Locale.ENGLISH)));
            }
        }
        catch (JSONException e) {
            fail("Get converted sql failed");
        }
    }

    private String getConvertedSql(JSONObject result)
    {
        String convertedSql = "";
        try {
            convertedSql = result.get(Constants.CONVERTED_SQL).toString();
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
            convertedSql = result.get(Constants.STATUS).toString();
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
            fail("The converted SQL can not be parsed by Hetu");
        }
    }
}
