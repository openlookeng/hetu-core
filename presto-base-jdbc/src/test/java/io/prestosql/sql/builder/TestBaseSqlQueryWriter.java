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
package io.prestosql.sql.builder;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.sql.builder.test.InMemoryJdbcDatabase;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.AbstractTestSqlQueryWriter;
import org.h2.Driver;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Optional;

import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestBaseSqlQueryWriter
        extends AbstractTestSqlQueryWriter
{
    private static final Logger LOGGER = Logger.get(TestBaseSqlQueryWriter.class);
    private InMemoryJdbcDatabase database;
    private LocalQueryRunner queryRunner;

    protected TestBaseSqlQueryWriter()
    {
        super(new BaseSqlQueryWriter());
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog(CONNECTOR_NAME)
                .setSchema(SCHEMA_NAME)
                .setSystemProperty("task_concurrency", "1").build());
    }

    @BeforeClass
    public void setup()
    {
        try {
            this.database = new InMemoryJdbcDatabase(new Driver(), "jdbc:h2:mem:tpch", CONNECTOR_NAME, SCHEMA_NAME, new BaseSqlQueryWriter());
            this.database.createTables();
            this.queryRunner.createCatalog(CONNECTOR_NAME, this.database.getConnectorFactory(), ImmutableMap.of());
            super.setup();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public void clean()
    {
        try {
            if (this.database != null) {
                this.database.close();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        super.clean();
    }

    @Override
    protected Optional<ConnectorFactory> getConnectorFactory()
    {
        return Optional.of(this.database.getConnectorFactory());
    }

    @Override
    protected void compare(String original, String rewritten)
    {
        MaterializedResult actualQueryResults = this.queryRunner.execute(original);
        MaterializedResult subQueryResults = this.queryRunner.execute(rewritten);
        assertEquals(subQueryResults.getMaterializedRows(), actualQueryResults.getMaterializedRows(),
                "result mismatch");
    }

    @Test
    public void testWindowFunction()
    {
        // the hetu sql grammar of functions window
        @Language("SQL")
        String query = "select quantity , max(quantity) over(partition by linestatus " +
                "order by returnflag desc nulls first rows 2 preceding) as ranking from lineitem order by quantity limit 100";
        assertStatement(query, "SELECT", "MAX", "over", "partition", "BY", "order", "by",
                "returnflag", "DESC", "NULLS", "FIRST", "ROWS", "2", "PRECEDING", "lineitem", "order", "BY", "quantity", "LIMIT");
    }

    @Test
    public void testGroupByWithComplexGroupingOperations()
    {
        // the hetu sql grammar of select#group-by-clause
        @Language("SQL")
        String query = "SELECT name, address, sum(acctbal) FROM customer GROUP BY rollup(name,address)";
        assertStatement(query, "SELECT", "sum", "acctbal", "GROUP", "BY", "GROUPING", "SETS", "name", "address", "name", "()");
    }

    @Test
    public void testIntersectStatement()
    {
        // the hetu sql grammar of select#union-intersect-except-clause
        LOGGER.info("Testing intersect statements");
        // For io.prestosql.sql.planner.iterative.rule.ImplementIntersectAsUnion change intersect operator to union all operater so addtion add "marker", "count" key words
        @Language("SQL") String queryIntersectDefault = "SELECT nationkey FROM nation INTERSECT SELECT regionkey FROM nation";
        assertStatement(queryIntersectDefault, "SELECT", "FROM", "nationkey", "nation", "UNION", "ALL", "marker", "count");

        @Language("SQL") String queryIntersectDistinct = "SELECT nationkey FROM nation INTERSECT DISTINCT SELECT regionkey FROM nation";
        assertStatement(queryIntersectDistinct, "SELECT", "FROM", "nationkey", "nation", "UNION", "ALL", "marker", "count");
    }

    @Test
    public void testExceptStatement()
    {
        // the hetu sql grammar of select#union-intersect-except-clause
        LOGGER.info("Testing except statements");
        // For io.prestosql.sql.planner.iterative.rule.ImplementIntersectAsUnion change intersect operator to union all operater so addtion add "marker", "count" key words
        @Language("SQL") String queryExceptDefault = "SELECT nationkey FROM nation EXCEPT SELECT regionkey FROM nation";
        assertStatement(queryExceptDefault, "SELECT", "FROM", "nationkey", "nation", "UNION", "ALL", "marker", "count");

        @Language("SQL") String queryExceptDistinct = "SELECT nationkey FROM nation EXCEPT DISTINCT SELECT regionkey FROM nation";
        assertStatement(queryExceptDistinct, "SELECT", "FROM", "nationkey", "nation", "UNION", "ALL", "marker", "count");
    }
}
