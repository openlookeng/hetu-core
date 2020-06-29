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

import static com.google.common.io.Files.asCharSource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
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
        parsingOptions = new ParsingOptions(DecimalLiteralTreatment.AS_DECIMAL);
        ConvertionOptions convertionOptions = new ConvertionOptions(SqlSyntaxType.HIVE, false);
        sqlConverter = SqlConverterFactory.getSqlConverter(convertionOptions);
    }

    @Test
    public void testCreateSchema() throws Exception
    {
        String sql = "CREATE SCHEMA IF NOT EXISTS S1 COMMENT 'hetu' LOCATION '/user'";
        String expectedSql = "CREATE SCHEMA IF NOT EXISTS S1\n" +
                "WITH (\n" +
                "   location = '/user'\n" +
                ")";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Warning");
        checkCanParsedByHetu(getConvertedSql(result));

        String sql2 = "CREATE SCHEMA IF NOT EXISTS S1 COMMENT 'hetu' LOCATION '/user' WITH DBPROPERTIES(NAME='user', ID=9)";
        assertEquals(getConversionStatus(sqlConverter.convert(sql2)), "Fail");
    }

    @Test
    public void testDropSchema() throws Exception
    {
        String sql1 = "drop schema S1 RESTRICT";
        String expectedSql = "DROP SCHEMA S1 RESTRICT";
        JSONObject result = sqlConverter.convert(sql1);
        assertEquals(getConvertedSql(result), expectedSql);
        checkCanParsedByHetu(getConvertedSql(result));

        String sql2 = "drop schema S1 CASCADE";
        assertEquals(getConversionStatus(sqlConverter.convert(sql2)), "Fail");
    }

    @Test
    public void testCreateTable() throws Exception
    {
        String sql = "create EXTERNAL table IF NOT EXISTS \n" +
                "p2 (id int, name string, level binary) \n" +
                "comment 'test' \n" +
                "partitioned by (score int, gender string) \n" +
                "clustered by (id, name) sorted by (name, level) into 3 buckets \n" +
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
                "   sorted_by = ARRAY['name','level'],\n" +
                "   bucket_count = 3,\n" +
                "   format = 'ORC',\n" +
                "   external = true,\n" +
                "   location = 'hdfs://xxx',\n" +
                "   transactional = true\n" +
                ")";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testCreateTableWithIllegalType() throws Exception
    {
        String sql1 = "create table t1 (a struct<b:int>)";
        assertEquals(getConversionStatus(sqlConverter.convert(sql1)), "Fail");

        String sql2 = "create table t1 (a uniontype<int, string>)";
        assertEquals(getConversionStatus(sqlConverter.convert(sql2)), "Fail");
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
        JSONObject result = sqlConverter.convert(sql1);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));

        String sql2 = "create table t1 (id int) STORED AS illegalType";
        assertEquals(getConversionStatus(sqlConverter.convert(sql2)), "Fail");
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
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
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
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testDropTable() throws Exception
    {
        String sql1 = "drop table IF EXISTS p2";
        JSONObject result = sqlConverter.convert(sql1);
        assertEquals(getConvertedSql(result), "DROP TABLE IF EXISTS p2");
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));

        String sql2 = "drop table IF EXISTS p2 purge";
        assertEquals(getConversionStatus(sqlConverter.convert(sql2)), "Fail");
    }

    @Test
    public void testInsertInto() throws Exception
    {
        String sql1 = "INSERT INTO TABLE T1 VALUES(10, \"NAME\")";
        String expectedSql1 = "INSERT INTO t1\n" +
                " VALUES \n" +
                "  ROW (10, 'NAME')\n";
        JSONObject result = sqlConverter.convert(sql1);
        assertEquals(getConvertedSql(result), expectedSql1);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));

        String sql2 = "INSERT INTO TABLE T2 SELECT ID, NAME FROM T1";
        String expectedSql2 = "INSERT INTO t2\n" +
                "SELECT\n" +
                "  ID\n" +
                ", NAME\n" +
                "FROM\n" +
                "  T1\n";
        result = sqlConverter.convert(sql2);
        assertEquals(getConvertedSql(result), expectedSql2);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
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
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testUpdate() throws Exception
    {
        String sql = "UPDATE T1 SET NAME='BOO' WHERE ID=2000";
        String expectedSql = "UPDATE t1 SET\n" +
                "name='BOO'\n" +
                "WHERE (ID = 2000)\n";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testDelete() throws Exception
    {
        String sql = "DELETE FROM T1 WHERE ID > 10";
        String expectedSql = "DELETE FROM T1 WHERE (ID > 10)";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testAlterTable() throws Exception
    {
        String sql1 = "ALTER TABLE T1 RENAME TO T2";
        String expectedSql1 = "ALTER TABLE t1 RENAME TO t2";
        JSONObject result = sqlConverter.convert(sql1);
        assertEquals(getConvertedSql(result), expectedSql1);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));

        String sql2 = "ALTER TABLE T1 SET TBLPROPERTIES (\"COMMENT\"=\"HETU\")";
        String expectedSql2 = "COMMENT ON TABLE t1 IS 'HETU'";
        result = sqlConverter.convert(sql2);
        assertEquals(getConvertedSql(result), expectedSql2);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testAddColumn() throws Exception
    {
        String sql = "alter table t100 add columns (name string comment 'hetu') RESTRICT";
        String expectedSql = "ALTER TABLE t100 ADD COLUMN name string COMMENT 'hetu'";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testCreateView() throws Exception
    {
        String sql = "CREATE VIEW V1 COMMENT 'HETU' AS SELECT (ID) FROM T1";
        String expectedSql = "CREATE VIEW V1 AS\n" +
                "SELECT ID\n" +
                "FROM\n" +
                "  T1\n";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(sqlConverter.convert(sql)), expectedSql);
        assertEquals(getConversionStatus(result), "Warning");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testDropView() throws Exception
    {
        String sql = "CREATE VIEW V1 COMMENT 'HETU' AS SELECT (ID) FROM T1";
        String expectedSql = "CREATE VIEW V1 AS\n" +
                "SELECT ID\n" +
                "FROM\n" +
                "  T1\n";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Warning");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testAlterView() throws Exception
    {
        String sql1 = "ALTER VIEW V1 AS SELECT * FROM T1";
        String expectedSql1 = "CREATE OR REPLACE VIEW V1 AS\n" +
                "SELECT *\n" +
                "FROM\n" +
                "  T1\n";
        JSONObject result = sqlConverter.convert(sql1);
        assertEquals(getConvertedSql(result), expectedSql1);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));

        String sql2 = "ALTER VIEW V1 SET TBLPROPERTIES (\"COMMENT\"=\"HETU\")";
        assertEquals(getConversionStatus(sqlConverter.convert(sql2)), "Fail");
    }

    @Test
    public void testCreateRole() throws Exception
    {
        String sql = "CREATE ROLE TEST";
        String expectedSql = "CREATE ROLE TEST";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testDropRole() throws Exception
    {
        String sql = "DROP ROLE TEST";
        String expectedSql = "DROP ROLE TEST";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testGrantRole() throws Exception
    {
        String sql = "GRANT ROLE admin TO USER TEST";
        String expectedSql = "GRANT admin TO USER TEST";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testRevokeRole() throws Exception
    {
        String sql = "revoke role admin from user test";
        String expectedSql = "REVOKE admin FROM USER test";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testSetRole() throws Exception
    {
        String sql = "SET ROLE ALL";
        String expectedSql = "SET ROLE ALL";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testGrant() throws Exception
    {
        String sql = "grant select on t100 to user test";
        String expectedSql = "GRANT select ON TABLE t100 TO USER test";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testRevoke() throws Exception
    {
        String sql = "REVOKE SELECT ON T100 FROM USER TEST";
        String expectedSql = "REVOKE SELECT ON TABLE t100 FROM USER TEST";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testShowGrants() throws Exception
    {
        String sql = "SHOW GRANT ON TABLE T1";
        String expectedSql = "SHOW GRANTS ON TABLE t1";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testExplain() throws Exception
    {
        String sql = "EXPLAIN ANALYZE SELECT * FROM T1";
        String expectedSql = "EXPLAIN ANALYZE \n" +
                "SELECT *\n" +
                "FROM\n" +
                "  T1\n";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testShowCreateTable() throws Exception
    {
        String sql = "SHOW CREATE TABLE T1";
        String expectedSql = "SHOW CREATE TABLE T1";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Warning");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testShowTables() throws Exception
    {
        String sql = "SHOW TABLES FROM TEST";
        String expectedSql = "SHOW TABLES FROM TEST";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testShowSchemas() throws Exception
    {
        String sql = "SHOW SCHEMAS";
        String expectedSql = "SHOW SCHEMAS";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testShowColumns() throws Exception
    {
        String sql = "SHOW COLUMNS FROM TEST";
        String expectedSql = "SHOW COLUMNS FROM TEST";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testDescribe() throws Exception
    {
        String sql1 = "DESC TEST";
        String expectedSql1 = "SHOW COLUMNS FROM TEST";
        JSONObject result = sqlConverter.convert(sql1);
        assertEquals(getConvertedSql(result), expectedSql1);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));

        String sql2 = "DESCRIBE TEST";
        String expectedSql2 = "SHOW COLUMNS FROM TEST";
        result = sqlConverter.convert(sql2);
        assertEquals(getConvertedSql(result), expectedSql2);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testShowRoles() throws Exception
    {
        String sql = "SHOW ROLES";
        String expectedSql = "SHOW ROLES";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testFunctions() throws Exception
    {
        String sql = "SHOW FUNCTIONS";
        String expectedSql = "SHOW FUNCTIONS";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
    }

    @Test
    public void testSet() throws Exception
    {
        String sql = "SET";
        String expectedSql = "SHOW SESSION";
        JSONObject result = sqlConverter.convert(sql);
        assertEquals(getConvertedSql(result), expectedSql);
        assertEquals(getConversionStatus(result), "Success");
        checkCanParsedByHetu(getConvertedSql(result));
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
}
