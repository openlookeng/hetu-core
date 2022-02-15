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

/*
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

package io.hetu.core.plugin.datacenter;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.hetu.core.statestore.StateStoreManagerPlugin;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.DistributedQueryRunner;
import io.prestosql.tests.QueryAssertions;
import io.prestosql.tests.ResultWithQueryId;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestCrossRegionDynamicFilter
{
    private static final Logger LOGGER = Logger.get(TestCrossRegionDynamicFilter.class);
    private static final String OPTIMIZE_DYNAMIC_FILTER_GENERATION = "optimize_dynamic_filter_generation";
    private final TestingPrestoServer remoteServer;
    private final DistributedQueryRunner queryRunner;
    private final Session enabledSession;
    private final Session disabledSession;

    public TestCrossRegionDynamicFilter()
            throws Exception
    {
        this.remoteServer = new TestingPrestoServer(ImmutableMap.<String, String>builder().put("node-scheduler.include-coordinator", "true")
                .put("hetu.embedded-state-store.enabled", "true")
                .put("hetu.data.center.split.count", "1").build());
        this.queryRunner = createDCQueryRunner(remoteServer, ImmutableMap.of());
        this.enabledSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("enable_dynamic_filtering", "true")
                .setSystemProperty("cross_region_dynamic_filter_enabled", "true")
                .setSystemProperty(OPTIMIZE_DYNAMIC_FILTER_GENERATION, "false")
                .build();
        this.disabledSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("enable_dynamic_filtering", "false")
                .setSystemProperty("cross_region_dynamic_filter_enabled", "false")
                .build();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        this.queryRunner.close();
        this.remoteServer.close();
    }

    @Test
    public void testJoinWithMultiFieldGroupBy()
    {
        assertQuery("SELECT orderstatus FROM dc.tpch.tiny.lineitem JOIN (SELECT DISTINCT orderkey, orderstatus FROM orders) T on dc.tpch.tiny.lineitem.orderkey = T.orderkey");
    }

    @Test
    public void testDistinctJoin()
    {
        assertQuery("SELECT a.orderstatus " +
                "FROM dc.tpch.tiny.orders a " +
                "JOIN customer b " +
                "ON a.custkey = b.custkey " +
                "where b.name='Customer#00001234' GROUP BY a.orderstatus");
    }

    @Test
    public void testJoinCoercion()
    {
        assertQuery("SELECT COUNT(*) FROM orders t JOIN (SELECT * FROM orders LIMIT 1) t2 ON sin(t2.custkey) = 0");
    }

    @Test
    public void testJoinCoercionOnEqualityComparison()
    {
        assertQuery("SELECT o.clerk, avg(o.shippriority), COUNT(l.linenumber) FROM orders o LEFT OUTER JOIN dc.tpch.tiny.lineitem l ON o.orderkey=l.orderkey AND o.shippriority=1 GROUP BY o.clerk");
    }

    @Test
    public void testJoinWithLessThanInJoinClause()
    {
        assertQuery("SELECT n.nationkey, r.regionkey FROM region r JOIN nation n ON n.regionkey = r.regionkey AND n.name < r.name");
        assertQuery("SELECT l.suppkey, n.nationkey, l.partkey, n.regionkey FROM nation n JOIN dc.tpch.tiny.lineitem l ON l.suppkey = n.nationkey AND l.partkey < n.regionkey");
        assertQuery("SELECT n.nationkey, r.regionkey FROM nation n JOIN region r ON n.regionkey = r.regionkey AND length(n.name) < length(substr(r.name, 5))");
    }

    @Test
    public void testJoinWithGreaterThanInJoinClause()
    {
        assertQuery("SELECT n.nationkey, r.regionkey FROM region r JOIN nation n ON n.regionkey = r.regionkey AND n.name > r.name AND r.regionkey = 0");
        assertQuery("SELECT l.suppkey, n.nationkey, l.partkey, n.regionkey FROM nation n JOIN dc.tpch.tiny.lineitem l ON l.suppkey = n.nationkey AND l.partkey > n.regionkey");
        assertQuery("SELECT n.nationkey, r.regionkey FROM nation n JOIN region r ON n.regionkey = r.regionkey AND length(n.name) > length(substr(r.name, 5))");
    }

    @Test
    public void testJoinWithRangePredicatesinJoinClause()
    {
        assertQuery("SELECT COUNT(*) " +
                "FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem " +
                "JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders " +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8 AND lineitem.linenumber % 2 = 0 " +
                "AND orders.custkey % 8 < 7 AND lineitem.suppkey % 10 < orders.custkey % 7 AND lineitem.suppkey % 7 > orders.custkey % 7");

        assertQuery("SELECT COUNT(*) " +
                "FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem " +
                "JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders " +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8 AND lineitem.linenumber % 2 = 0 " +
                "AND orders.custkey % 8 < lineitem.linenumber % 2 AND lineitem.suppkey % 10 < orders.custkey % 7 AND lineitem.suppkey % 7 > orders.custkey % 7");
    }

    @Test
    public void testJoinWithMultipleLessThanPredicatesDifferentOrders()
    {
        // test that fast inequality join is not sensitive to order of search conjuncts.
        assertQuery("SELECT count(*) FROM dc.tpch.tiny.lineitem l JOIN nation n ON l.suppkey % 5 = n.nationkey % 5 AND l.partkey % 3 < n.regionkey AND l.partkey % 3 + 1 < n.regionkey AND l.partkey % 3 + 2 < n.regionkey");
        assertQuery("SELECT count(*) FROM dc.tpch.tiny.lineitem l JOIN nation n ON l.suppkey % 5 = n.nationkey % 5 AND l.partkey % 3 + 2 < n.regionkey AND l.partkey % 3 + 1 < n.regionkey AND l.partkey % 3 < n.regionkey");
        assertQuery("SELECT count(*) FROM dc.tpch.tiny.lineitem l JOIN nation n ON l.suppkey % 5 = n.nationkey % 5 AND l.partkey % 3 > n.regionkey AND l.partkey % 3 + 1 > n.regionkey AND l.partkey % 3 + 2 > n.regionkey");
        assertQuery("SELECT count(*) FROM dc.tpch.tiny.lineitem l JOIN nation n ON l.suppkey % 5 = n.nationkey % 5 AND l.partkey % 3 + 2 > n.regionkey AND l.partkey % 3 + 1 > n.regionkey AND l.partkey % 3 > n.regionkey");
    }

    @Test
    public void testJoinWithLessThanOnDatesInJoinClause()
    {
        assertQuery(
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM orders o JOIN dc.tpch.tiny.lineitem l ON l.orderkey = o.orderkey AND l.shipdate < o.orderdate + INTERVAL '10' DAY");
        assertQuery(
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM dc.tpch.tiny.lineitem l JOIN orders o ON l.orderkey = o.orderkey AND l.shipdate < DATE_ADD('DAY', 10, o.orderdate)");
        assertQuery(
                "SELECT o.orderkey, o.orderdate, l.shipdate FROM orders o JOIN dc.tpch.tiny.lineitem l ON o.orderkey=l.orderkey AND o.orderdate + INTERVAL '2' DAY <= l.shipdate AND l.shipdate < o.orderdate + INTERVAL '7' DAY");
    }

    @Test
    public void testSimpleJoin()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey");
        assertQuery("" +
                "SELECT COUNT(*) FROM " +
                "(SELECT orderkey FROM dc.tpch.tiny.lineitem WHERE orderkey < 1000) a " +
                "JOIN " +
                "(SELECT orderkey FROM orders WHERE orderkey < 2000) b " +
                "ON NOT (a.orderkey <= b.orderkey)");
    }

    @Test
    public void testJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = 2");
    }

    @Test
    public void testJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON orders.orderkey = 2");
    }

    @Test
    public void testSimpleJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.orderkey = 2");
    }

    @Test
    public void testJoinDoubleClauseWithLeftOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testJoinDoubleClauseWithRightOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.orderkey = dc.tpch.tiny.lineitem.partkey");
    }

    @Test
    public void testJoinWithAlias()
    {
        assertQuery("SELECT * FROM (dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey) x");
    }

    @Test
    public void testJoinWithConstantExpression()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND 123 = 123");
    }

    @Test
    public void testJoinWithConstantTrueExpressionWithCoercion()
    {
        assertQuery("SELECT count(*) > 0 FROM nation JOIN region ON (cast(1.2 AS real) = CAST(1.2 AS decimal(2,1)))");
    }

    @Test
    public void testJoinWithCanonicalizedConstantTrueExpressionWithCoercion()
    {
        assertQuery("SELECT count(*) > 0 FROM nation JOIN region ON CAST((CASE WHEN (TRUE IS NOT NULL) THEN '1.2' ELSE '1.2' END) AS real) = CAST(1.2 AS decimal(2,1))");
    }

    @Test
    public void testJoinWithConstantPredicatePushDown()
    {
        assertQuery("" +
                "SELECT\n" +
                "  a.orderstatus\n" +
                "  , a.clerk\n" +
                "FROM (\n" +
                "  SELECT DISTINCT orderstatus, clerk FROM orders\n" +
                ") a\n" +
                "INNER JOIN (\n" +
                "  SELECT DISTINCT orderstatus, clerk FROM orders\n" +
                ") b\n" +
                "ON\n" +
                "  a.orderstatus = b.orderstatus\n" +
                "  and a.clerk = b.clerk\n" +
                "where a.orderstatus = 'F'\n");
    }

    @Test
    public void testJoinWithInferredFalseJoinClause()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM orders\n" +
                "JOIN dc.tpch.tiny.lineitem\n" +
                "ON CAST(orders.orderkey AS VARCHAR) = CAST(dc.tpch.tiny.lineitem.orderkey AS VARCHAR)\n" +
                "WHERE orders.orderkey = 1 AND dc.tpch.tiny.lineitem.orderkey = 2\n");
    }

    @Test
    public void testJoinUsing()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders USING (orderkey)");
    }

    @Test
    public void testJoinWithReversedComparison()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON orders.orderkey = dc.tpch.tiny.lineitem.orderkey");
    }

    @Test
    public void testJoinWithComplexExpressions()
    {
        assertQuery("SELECT SUM(custkey) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = CAST(orders.orderkey AS BIGINT)");
    }

    @Test
    public void testJoinWithComplexExpressions2()
    {
        assertQuery(
                "SELECT SUM(custkey) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = CASE WHEN orders.custkey = 1 and orders.orderstatus = 'F' THEN orders.orderkey ELSE NULL END");
    }

    @Test
    public void testJoinWithComplexExpressions3()
    {
        assertQuery("SELECT SUM(custkey) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey + 1 = orders.orderkey + 1");
    }

    @Test
    public void testJoinWithNormalization()
    {
        assertQuery("SELECT COUNT(*) FROM nation a JOIN nation b on not ((a.nationkey + b.nationkey) <> b.nationkey)");
        assertQuery("SELECT COUNT(*) FROM nation a JOIN nation b on not (a.nationkey <> b.nationkey)");
        assertQuery("SELECT COUNT(*) FROM nation a JOIN nation b on not (a.nationkey = b.nationkey)");
        assertQuery("SELECT COUNT(*) FROM nation a JOIN nation b on not (not CAST(a.nationkey AS boolean))");
        assertQuery("SELECT COUNT(*) FROM nation a JOIN nation b on not not not (a.nationkey = b.nationkey)");
    }

    @Test
    public void testSelfJoin()
    {
        assertQuery("SELECT COUNT(*) FROM orders a JOIN orders b on a.orderkey = b.orderkey");
    }

    @Test
    public void testWildcardFromJoin()
    {
        assertQuery(
                "SELECT * FROM (SELECT orderkey, partkey FROM dc.tpch.tiny.lineitem) a JOIN (SELECT orderkey, custkey FROM orders) b using (orderkey)");
    }

    @Test
    public void testQualifiedWildcardFromJoin()
    {
        assertQuery(
                "SELECT a.*, b.* FROM (SELECT orderkey, partkey FROM dc.tpch.tiny.lineitem) a JOIN (SELECT orderkey, custkey FROM orders) b using (orderkey)");
    }

    @Test
    public void testJoinAggregations()
    {
        assertQuery(
                "SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b.orderdate");
    }

    @Test
    public void testNonEqualityJoin()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.quantity + length(orders.comment) > 7");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND NOT dc.tpch.tiny.lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON NOT NOT dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND NOT NOT dc.tpch.tiny.lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND NOT NOT NOT dc.tpch.tiny.lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.quantity <= 2");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.quantity != 2");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.shipdate > orders.orderdate");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.orderdate < dc.tpch.tiny.lineitem.shipdate");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.comment LIKE '%forges%'");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.comment LIKE dc.tpch.tiny.lineitem.comment");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.comment LIKE '%forges%'");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.comment LIKE orders.comment");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.comment NOT LIKE '%forges%'");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.comment NOT LIKE dc.tpch.tiny.lineitem.comment");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND NOT (orders.comment LIKE '%forges%')");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND NOT (orders.comment LIKE dc.tpch.tiny.lineitem.comment)");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.quantity + length(orders.comment) > 7");
    }

    @Test
    public void testNonEqualityLeftJoin()
    {
        assertQuery("SELECT COUNT(*) FROM " +
                "      (SELECT * FROM dc.tpch.tiny.lineitem ORDER BY orderkey,linenumber LIMIT 5) l " +
                "         LEFT OUTER JOIN " +
                "      (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o " +
                "         ON " +
                "      o.custkey != 1000 WHERE o.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem LEFT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem LEFT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.custkey > 1000.0 WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem LEFT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.custkey > orders.totalprice WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem LEFT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.custkey > dc.tpch.tiny.lineitem.quantity WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem LEFT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.quantity > 5 WHERE orders.orderkey IS NULL");
    }

    @Test
    public void testLeftJoinWithEmptyInnerTable()
    {
        // Use orderkey = rand() to create an empty relation
        assertQuery("SELECT * FROM dc.tpch.tiny.lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON a.orderkey = b.orderkey");
        assertQuery("SELECT * FROM dc.tpch.tiny.lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON a.orderkey > b.orderkey");
        assertQuery("SELECT * FROM dc.tpch.tiny.lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON 1 = 1");
        assertQuery("SELECT * FROM dc.tpch.tiny.lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON b.orderkey > 1");
        assertQuery("SELECT * FROM dc.tpch.tiny.lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON b.orderkey > b.totalprice");
    }

    @Test
    public void testRightJoinWithEmptyInnerTable()
    {
        // Use orderkey = rand() to create an empty relation
        assertQuery("SELECT * FROM orders b RIGHT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON a.orderkey = b.orderkey");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON a.orderkey > b.orderkey");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON 1 = 1");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON b.orderkey > 1");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON b.orderkey > b.totalprice");
    }

    @Test
    public void testNonEqualityRightJoin()
    {
        assertQuery("SELECT COUNT(*) FROM " +
                "      (SELECT * FROM dc.tpch.tiny.lineitem ORDER BY orderkey,linenumber LIMIT 5) l " +
                "         RIGHT OUTER JOIN " +
                "      (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o " +
                "         ON " +
                "      l.quantity != 5 WHERE l.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.quantity > 5 WHERE dc.tpch.tiny.lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.quantity > 5.0 WHERE dc.tpch.tiny.lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.quantity > dc.tpch.tiny.lineitem.suppkey WHERE dc.tpch.tiny.lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.quantity*1000 > orders.totalprice WHERE dc.tpch.tiny.lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.totalprice > 1000 WHERE dc.tpch.tiny.lineitem.orderkey IS NULL");
    }

    @Test
    public void testNonEqualityFullJoin()
    {
        assertQuery(
                "SELECT COUNT(*) FROM dc.tpch.tiny.lineitem FULL JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.quantity > 5 WHERE dc.tpch.tiny.lineitem.orderkey IS NULL OR orders.orderkey IS NULL");
        assertQuery(
                "SELECT COUNT(*) FROM dc.tpch.tiny.lineitem FULL OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 WHERE dc.tpch.tiny.lineitem.orderkey IS NULL OR orders.orderkey IS NULL");
        assertQuery(
                "SELECT COUNT(*) FROM dc.tpch.tiny.lineitem FULL OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.custkey > dc.tpch.tiny.lineitem.quantity WHERE dc.tpch.tiny.lineitem.orderkey IS NULL OR orders.orderkey IS NULL");
    }

    @Test
    public void testJoinOnMultipleFields()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.shipdate = orders.orderdate");
    }

    @Test
    public void testJoinUsingMultipleFields()
    {
        assertQuery(
                "SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN (SELECT orderkey, orderdate shipdate FROM orders) T USING (orderkey, shipdate)");
    }

    @Test
    public void testColocatedJoinWithLocalUnion()
    {
        assertQuery(
                "SELECT count(*) FROM ((SELECT * FROM orders) union all (SELECT * FROM orders)) JOIN orders USING (orderkey)");
    }

    @Test
    public void testJoinWithNonJoinExpression()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.custkey = 1");
    }

    @Test
    public void testJoinWithNullValues()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM dc.tpch.tiny.lineitem\n" +
                "  WHERE partkey % 512 = 0\n" +
                ") AS lineitem \n" +
                "JOIN (\n" +
                "  SELECT CASE WHEN orderkey % 2 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 512 = 0\n" +
                ") AS orders\n" +
                "ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testLeftFilteredJoin()
    {
        // Test predicate move around
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 2 = 0) a JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testRightFilteredJoin()
    {
        // Test predicate move around
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM dc.tpch.tiny.lineitem JOIN (SELECT *  FROM orders WHERE orderkey % 2 = 0) a ON dc.tpch.tiny.lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testJoinWithFullyPushedDownJoinClause()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem JOIN orders ON orders.custkey = 1 AND dc.tpch.tiny.lineitem.orderkey = 1");
    }

    @Test
    public void testJoinPredicateMoveAround()
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8 AND lineitem.linenumber % 2 = 0\n" +
                "WHERE orders.custkey % 8 < 7 AND orders.custkey % 8 = lineitem.orderkey % 8 AND lineitem.suppkey % 7 > orders.custkey % 7");
    }

    @Test
    public void testSimpleFullJoin()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem FULL JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem FULL OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem FULL JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testFullJoinNormalizedToLeft()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem FULL JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey WHERE dc.tpch.tiny.lineitem.orderkey IS NOT NULL");

        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem FULL JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.custkey WHERE dc.tpch.tiny.lineitem.orderkey IS NOT NULL");
    }

    @Test
    public void testFullJoinNormalizedToRight()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem FULL JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey WHERE orders.orderkey IS NOT NULL");

        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem FULL JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.custkey WHERE orders.custkey IS NOT NULL");
    }

    @Test
    public void testFullJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 1024 = 0) lineitem FULL JOIN orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testFullJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 1024 = 0) lineitem FULL JOIN orders ON orders.orderkey = 1024");
    }

    @Test
    public void testSimpleFullJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem FULL JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleFullJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem FULL JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.orderkey = 2");
    }

    @Test
    public void testOuterJoinWithNullsOnProbe()
    {
        assertQuery(
                "SELECT DISTINCT a.orderkey FROM " +
                        "(SELECT CASE WHEN orderkey > 10 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "RIGHT OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey");

        assertQuery(
                "SELECT DISTINCT a.orderkey FROM " +
                        "(SELECT CASE WHEN orderkey > 2 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "FULL OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey");
    }

    @Test
    public void testSimpleLeftJoin()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem LEFT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem LEFT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testLeftJoinNormalizedToInner()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem LEFT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey WHERE orders.orderkey IS NOT NULL");
    }

    @Test
    public void testLeftJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testLeftJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN orders ON orders.orderkey = 1024");
    }

    @Test
    public void testSimpleLeftJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem LEFT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleLeftJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem LEFT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.orderkey = 2");
    }

    @Test
    public void testDoubleFilteredLeftJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredLeftJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testLeftJoinDoubleClauseWithLeftOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem LEFT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testLeftJoinDoubleClauseWithRightOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem LEFT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.orderkey = dc.tpch.tiny.lineitem.partkey");
    }

    @Test
    public void testBuildFilteredLeftJoin()
    {
        assertQuery("SELECT * FROM dc.tpch.tiny.lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 2 = 0) a ON dc.tpch.tiny.lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testProbeFilteredLeftJoin()
    {
        assertQuery("SELECT * FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 2 = 0) a LEFT JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testLeftJoinPredicateMoveAround()
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "LEFT JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8\n" +
                "WHERE (orders.custkey % 8 < 7 OR orders.custkey % 8 IS NULL) AND orders.custkey % 8 = lineitem.orderkey % 8");
    }

    @Test
    public void testLeftJoinEqualityInference()
    {
        // Test that we can infer orders.orderkey % 4 = orders.custkey % 3 on the inner side
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 4 = 0 AND suppkey % 2 = partkey % 2 AND linenumber % 3 = orderkey % 3) lineitem\n" +
                "LEFT JOIN (SELECT * FROM orders WHERE orderkey % 4 = 0) orders\n" +
                "ON lineitem.linenumber % 3 = orders.orderkey % 4 AND lineitem.orderkey % 3 = orders.custkey % 3\n" +
                "WHERE lineitem.suppkey % 2 = lineitem.linenumber % 3");
    }

    @Test
    public void testLeftJoinWithNullValues()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM dc.tpch.tiny.lineitem\n" +
                "  WHERE partkey % 512 = 0\n" +
                ") AS lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT CASE WHEN orderkey % 2 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 512 = 0\n" +
                ") AS orders\n" +
                "ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testSimpleRightJoin()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey");

        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.custkey");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT OUTER JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testRightJoinNormalizedToInner()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey WHERE dc.tpch.tiny.lineitem.orderkey IS NOT NULL");
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.custkey WHERE dc.tpch.tiny.lineitem.orderkey IS NOT NULL");
    }

    @Test
    public void testRightJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testRightJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredRightJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredRightJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testSimpleRightJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleRightJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.orderkey = 2");
    }

    @Test
    public void testRightJoinDoubleClauseWithLeftOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND dc.tpch.tiny.lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testRightJoinDoubleClauseWithRightOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM dc.tpch.tiny.lineitem RIGHT JOIN orders ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey AND orders.orderkey = dc.tpch.tiny.lineitem.partkey");
    }

    @Test
    public void testBuildFilteredRightJoin()
    {
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 2 = 0) a RIGHT JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testProbeFilteredRightJoin()
    {
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM dc.tpch.tiny.lineitem RIGHT JOIN (SELECT *  FROM orders WHERE orderkey % 2 = 0) a ON dc.tpch.tiny.lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testRightJoinPredicateMoveAround()
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "RIGHT JOIN (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8\n" +
                "WHERE (orders.custkey % 8 < 7 OR orders.custkey % 8 IS NULL) AND orders.custkey % 8 = lineitem.orderkey % 8");
    }

    @Test
    public void testRightJoinEqualityInference()
    {
        // Test that we can infer orders.orderkey % 4 = orders.custkey % 3 on the inner side
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE orderkey % 4 = 0) orders\n" +
                "RIGHT JOIN (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey % 4 = 0 AND suppkey % 2 = partkey % 2 AND linenumber % 3 = orderkey % 3) lineitem\n" +
                "ON lineitem.linenumber % 3 = orders.orderkey % 4 AND lineitem.orderkey % 3 = orders.custkey % 3\n" +
                "WHERE lineitem.suppkey % 2 = lineitem.linenumber % 3");
    }

    @Test
    public void testRightJoinWithNullValues()
    {
        assertQuery("" +
                "SELECT lineitem.orderkey, orders.orderkey\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM dc.tpch.tiny.lineitem\n" +
                "  WHERE partkey % 512 = 0\n" +
                ") AS lineitem \n" +
                "RIGHT JOIN (\n" +
                "  SELECT CASE WHEN orderkey % 2 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 512 = 0\n" +
                ") AS orders\n" +
                "ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testJoinWithStatefulFilterFunction()
    {
        long joinOutputRowCount = 60175;
        assertQuery(format("SELECT count(*) FROM dc.tpch.tiny.lineitem l LEFT OUTER JOIN orders o ON l.orderkey = o.orderkey AND stateful_sleeping_sum(%s, 100, l.linenumber, o.shippriority) > 0",
                10 * 1. / joinOutputRowCount));
    }

    @Test
    public void testJoinWithGroupByAsProbe()
    {
        // we join on customer key instead of order key because
        // orders is effectively distributed on order key due the
        // generated data being sorted
        assertQuery("SELECT " +
                "  b.orderkey, " +
                "  b.custkey, " +
                "  a.custkey " +
                "FROM ( " +
                "  SELECT custkey" +
                "  FROM orders " +
                "  GROUP BY custkey" +
                ") a " +
                "JOIN orders b " +
                "  ON a.custkey = b.custkey ");
    }

    @Test
    public void testJoinEffectivePredicateWithNoRanges()
    {
        assertQuery("" +
                "SELECT * FROM orders a " +
                "   JOIN (SELECT * FROM orders WHERE orderkey IS NULL) b " +
                "   ON a.orderkey = b.orderkey");
    }

    @Test
    public void testJoinUnaliasedSubqueries()
    {
        assertQuery(
                "SELECT COUNT(*) FROM (SELECT * FROM dc.tpch.tiny.lineitem) JOIN (SELECT * FROM orders) USING (orderkey)");
    }

    @Test
    public void testWithSelfJoin()
    {
        assertQuery("WITH x AS (SELECT DISTINCT orderkey FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "SELECT count(*) FROM x a JOIN x b USING (orderkey)");
    }

    @Test
    public void testJoinProjectionPushDown()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM\n" +
                "  (SELECT orderkey, abs(orderkey) a FROM orders) t\n" +
                "JOIN\n" +
                "  (SELECT orderkey, abs(orderkey) a FROM orders) u\n" +
                "ON\n" +
                "  t.orderkey = u.orderkey");
    }

    @Test
    public void testRandCrossJoins()
    {
        assertQuery("" +
                "SELECT COUNT(*) " +
                "FROM (SELECT * FROM orders ORDER BY rand() LIMIT 5) a " +
                "CROSS JOIN (SELECT * FROM dc.tpch.tiny.lineitem ORDER BY rand() LIMIT 5) b");
    }

    @Test
    public void testCrossJoins()
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a " +
                "CROSS JOIN (SELECT * FROM dc.tpch.tiny.lineitem ORDER BY orderkey LIMIT 5) b");
    }

    @Test
    public void testCrossJoinEmptyProbePage()
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders WHERE orderkey < 0) a " +
                "CROSS JOIN (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey < 100) b");
    }

    @Test
    public void testCrossJoinEmptyBuildPage()
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders WHERE orderkey < 100) a " +
                "CROSS JOIN (SELECT * FROM dc.tpch.tiny.lineitem WHERE orderkey < 0) b");
    }

    @Test
    public void testImplicitCrossJoin()
    {
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 3) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 4) b");
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 2) b");
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 5) b, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 5) c ");

        // Inner Join converted to cross join because all join conditions are pushed down.
        assertQuery("" +
                "SELECT l.orderkey, l.linenumber " +
                "FROM orders o INNER JOIN dc.tpch.tiny.lineitem l " +
                "ON o.custkey = l.linenumber " +
                "WHERE o.custkey IN (5) AND l.orderkey IN (7522)");

        assertQuery("" +
                "SELECT o.custkey " +
                "FROM orders o INNER JOIN dc.tpch.tiny.lineitem l " +
                "ON o.custkey = l.linenumber " +
                "WHERE o.custkey IN (5) AND l.orderkey IN (7522)");

        assertQuery("" +
                "SELECT COUNT(*) " +
                "FROM orders o INNER JOIN dc.tpch.tiny.lineitem l " +
                "ON o.custkey = l.linenumber " +
                "WHERE o.custkey IN (5) AND l.orderkey IN (7522)");
    }

    @Test
    public void testSemiJoin()
    {
        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM dc.tpch.tiny.lineitem " +
                "GROUP BY linenumber " +
                "HAVING min(orderkey) IN (SELECT orderkey FROM orders WHERE orderkey > 1)");

        // Throw in a bunch of IN subquery predicates
        assertQuery("" +
                "SELECT *, o2.custkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM dc.tpch.tiny.lineitem\n" +
                "    WHERE orderkey % 5 = 0)\n" +
                "FROM (SELECT * FROM orders WHERE custkey % 256 = 0) o1\n" +
                "JOIN (SELECT * FROM orders WHERE custkey % 256 = 0) o2\n" +
                "  ON (o1.orderkey IN (SELECT orderkey FROM dc.tpch.tiny.lineitem WHERE orderkey % 4 = 0)) = (o2.orderkey IN (SELECT orderkey FROM dc.tpch.tiny.lineitem WHERE orderkey % 4 = 0))\n" +
                "WHERE o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM dc.tpch.tiny.lineitem\n" +
                "    WHERE orderkey % 4 = 0)\n" +
                "ORDER BY o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM dc.tpch.tiny.lineitem\n" +
                "    WHERE orderkey % 7 = 0)");
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM dc.tpch.tiny.lineitem\n" +
                "    WHERE partkey % 4 = 0),\n" +
                "  SUM(\n" +
                "    CASE\n" +
                "      WHEN orderkey\n" +
                "        IN (\n" +
                "          SELECT orderkey\n" +
                "          FROM dc.tpch.tiny.lineitem\n" +
                "          WHERE suppkey % 4 = 0)\n" +
                "      THEN 1\n" +
                "      ELSE 0\n" +
                "      END)\n" +
                "FROM orders\n" +
                "GROUP BY orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM dc.tpch.tiny.lineitem\n" +
                "    WHERE partkey % 4 = 0)\n" +
                "HAVING SUM(\n" +
                "  CASE\n" +
                "    WHEN orderkey\n" +
                "      IN (\n" +
                "        SELECT orderkey\n" +
                "        FROM dc.tpch.tiny.lineitem\n" +
                "        WHERE suppkey % 4 = 0)\n" +
                "      THEN 1\n" +
                "      ELSE 0\n" +
                "      END) > 1");
    }

    @Test
    public void testJoinConstantPropagation()
    {
        assertQuery("" +
                "SELECT x, y, COUNT(*)\n" +
                "FROM (SELECT orderkey, 0 AS x FROM orders) a \n" +
                "JOIN (SELECT orderkey, 1 AS y FROM orders) b \n" +
                "ON a.orderkey = b.orderkey\n" +
                "GROUP BY 1, 2");
    }

    @Test
    public void testAntiJoin()
    {
        assertQuery("" +
                "SELECT *, orderkey\n" +
                "  NOT IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM dc.tpch.tiny.lineitem\n" +
                "    WHERE orderkey % 3 = 0)\n" +
                "FROM orders");
    }

    @Test
    public void testSemiJoinLimitPushDown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM dc.tpch.tiny.lineitem\n" +
                "    WHERE orderkey % 2 = 0)\n" +
                "  FROM orders\n" +
                "  LIMIT 10)");
    }

    @Test
    public void testSemiJoinNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM dc.tpch.tiny.lineitem)\n" +
                "FROM orders");
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM dc.tpch.tiny.lineitem)\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders)");
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM dc.tpch.tiny.lineitem)\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 4 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders)");
    }

    @Test
    public void testSemiJoinWithGroupBy()
    {
        // using the same subquery in query
        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM dc.tpch.tiny.lineitem " +
                "GROUP BY linenumber");

        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM dc.tpch.tiny.lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM dc.tpch.tiny.lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM dc.tpch.tiny.lineitem " +
                "GROUP BY linenumber " +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM dc.tpch.tiny.lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        // using different subqueries
        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM dc.tpch.tiny.lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT sum(orderkey) FROM orders WHERE orderkey < 5)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM dc.tpch.tiny.lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey > 3)");

        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM dc.tpch.tiny.lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 5)" +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey > 3)");
    }

    @Test
    public void testSemiJoinUnionNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 500 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM orders\n" +
                "    WHERE orderkey % 200 = 0\n" +
                "    UNION ALL\n" +
                "    SELECT CASE WHEN orderkey % 600 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM orders\n" +
                "    WHERE orderkey % 300 = 0\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM dc.tpch.tiny.lineitem\n" +
                "  WHERE orderkey % 100 = 0)");
    }

    @Test
    public void testSemiJoinAggregationNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 10 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM dc.tpch.tiny.lineitem\n" +
                "    WHERE orderkey % 2 = 0\n" +
                "    GROUP BY orderkey\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 3 = 0)");
    }

    @Test
    public void testSemiJoinUnionAggregationNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 500 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM dc.tpch.tiny.lineitem\n" +
                "    WHERE orderkey % 250 = 0\n" +
                "    UNION ALL\n" +
                "    SELECT CASE WHEN orderkey % 300 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM dc.tpch.tiny.lineitem\n" +
                "    WHERE orderkey % 200 = 0\n" +
                "    GROUP BY orderkey\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 100 = 0)\n");
    }

    @Test
    public void testSemiJoinAggregationUnionNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM (\n" +
                "      SELECT CASE WHEN orderkey % 500 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "      FROM orders\n" +
                "      WHERE orderkey % 200 = 0\n" +
                "      UNION ALL\n" +
                "      SELECT CASE WHEN orderkey % 600 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "      FROM orders\n" +
                "      WHERE orderkey % 300 = 0\n" +
                "    )\n" +
                "    GROUP BY orderkey\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM dc.tpch.tiny.lineitem\n" +
                "  WHERE orderkey % 100 = 0)");
    }

    @Test
    public void testJoinPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM dc.tpch.tiny.lineitem \n" +
                "JOIN (\n" +
                "  SELECT * FROM orders\n" +
                ") orders \n" +
                "ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey % 4 = 0\n" +
                "  AND dc.tpch.tiny.lineitem.suppkey > orders.orderkey");
    }

    @Test
    public void testLeftJoinAsInnerPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM dc.tpch.tiny.lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey % 4 = 0\n" +
                "  AND (dc.tpch.tiny.lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.custkey IS NULL)");
    }

    @Test
    public void testPlainLeftJoinPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM dc.tpch.tiny.lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey \n" +
                "WHERE dc.tpch.tiny.lineitem.orderkey % 4 = 0\n" +
                "  AND (dc.tpch.tiny.lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testLeftJoinPredicatePushdownWithSelfEquality()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM dc.tpch.tiny.lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey = orders.orderkey\n" +
                "  AND dc.tpch.tiny.lineitem.orderkey % 4 = 0\n" +
                "  AND (dc.tpch.tiny.lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testLeftJoinPredicatePushdownWithNullConstant()
    {
        assertQuery("" +
                "SELECT count(*)\n" +
                "FROM orders a\n" +
                "LEFT OUTER JOIN orders b\n" +
                "  ON a.clerk = b.clerk\n" +
                "WHERE a.orderpriority='5-LOW'\n" +
                "  AND b.orderpriority='1-URGENT'\n" +
                "  AND b.clerk is null\n" +
                "  AND a.orderkey % 4 = 0\n");
    }

    @Test
    public void testRightJoinAsInnerPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders\n" +
                "RIGHT JOIN dc.tpch.tiny.lineitem\n" +
                "ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey % 4 = 0\n" +
                "  AND (dc.tpch.tiny.lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.custkey IS NULL)");
    }

    @Test
    public void testPlainRightJoinPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "RIGHT JOIN dc.tpch.tiny.lineitem\n" +
                "ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey \n" +
                "WHERE dc.tpch.tiny.lineitem.orderkey % 4 = 0\n" +
                "  AND (dc.tpch.tiny.lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testRightJoinPredicatePushdownWithSelfEquality()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "RIGHT JOIN dc.tpch.tiny.lineitem\n" +
                "ON dc.tpch.tiny.lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey = orders.orderkey\n" +
                "  AND dc.tpch.tiny.lineitem.orderkey % 4 = 0\n" +
                "  AND (dc.tpch.tiny.lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testPredicatePushdownJoinEqualityGroups()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT custkey custkey1, custkey%4 custkey1a, custkey%8 custkey1b, custkey%16 custkey1c\n" +
                "  FROM orders\n" +
                ") orders1 \n" +
                "JOIN (\n" +
                "  SELECT custkey custkey2, custkey%4 custkey2a, custkey%8 custkey2b\n" +
                "  FROM orders\n" +
                ") orders2 ON orders1.custkey1 = orders2.custkey2\n" +
                "WHERE custkey2a = custkey2b\n" +
                "  AND custkey1 = custkey1a\n" +
                "  AND custkey2 = custkey2a\n" +
                "  AND custkey1a = custkey1c\n" +
                "  AND custkey1b = custkey1c\n" +
                "  AND custkey1b % 2 = 0");
    }

    @Test
    public void testSemiJoinPredicateMoveAround()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE custkey % 2 = 0 AND orderkey % 3 = 0)\n" +
                "WHERE orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 7 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM dc.tpch.tiny.lineitem\n" +
                "    WHERE partkey % 2 = 0)\n" +
                "  AND\n" +
                "    orderkey % 2 = 0");
    }

    @Test
    public void testRightJoinWithEmptyBuildSide()
    {
        assertQuery("WITH small_part AS (SELECT * FROM part WHERE name = 'a') SELECT dc.tpch.tiny.lineitem.orderkey FROM dc.tpch.tiny.lineitem RIGHT JOIN small_part ON dc.tpch.tiny.lineitem.partkey = small_part.partkey");
    }

    @Test
    public void testLeftJoinWithEmptyBuildSide()
    {
        assertQuery("WITH small_part AS (SELECT * FROM part WHERE name = 'a') SELECT dc.tpch.tiny.lineitem.orderkey FROM dc.tpch.tiny.lineitem LEFT JOIN small_part ON dc.tpch.tiny.lineitem.partkey = small_part.partkey");
    }

    @Test
    public void testFullJoinWithEmptyBuildSide()
    {
        assertQuery("WITH small_part AS (SELECT * FROM part WHERE name = 'a') SELECT dc.tpch.tiny.lineitem.orderkey FROM dc.tpch.tiny.lineitem FULL OUTER JOIN small_part ON dc.tpch.tiny.lineitem.partkey = small_part.partkey");
    }

    @Test
    public void testInnerJoinWithEmptyProbeSide()
    {
        assertQuery("WITH small_part AS (SELECT * FROM part WHERE name = 'a') SELECT dc.tpch.tiny.lineitem.orderkey FROM small_part INNER JOIN dc.tpch.tiny.lineitem ON small_part.partkey = dc.tpch.tiny.lineitem.partkey");
    }

    @Test
    public void testRightJoinWithEmptyProbeSide()
    {
        assertQuery("WITH small_part AS (SELECT * FROM part WHERE name = 'a') SELECT dc.tpch.tiny.lineitem.orderkey FROM small_part RIGHT JOIN dc.tpch.tiny.lineitem ON  small_part.partkey = dc.tpch.tiny.lineitem.partkey");
    }

    private void assertQuery(String sql)
    {
        long disabledTime = 0;
        this.queryRunner.executeWithQueryId(disabledSession, sql);
        this.queryRunner.executeWithQueryId(disabledSession, sql);

        disabledTime = System.currentTimeMillis();
        ResultWithQueryId<MaterializedResult> expected = this.queryRunner.executeWithQueryId(disabledSession, sql);
        disabledTime = System.currentTimeMillis() - disabledTime;

        long enabledTime = System.currentTimeMillis();
        ResultWithQueryId<MaterializedResult> actual = this.queryRunner.executeWithQueryId(enabledSession, sql);
        enabledTime = System.currentTimeMillis() - enabledTime;

        LOGGER.info("Ran '%s' without cross region df in %d ms and with cross region df in %d ms", sql, disabledTime, enabledTime);
        QueryAssertions.assertEqualsIgnoreOrder(actual.getResult(), expected.getResult(), "incorrect result");
    }

    private static DistributedQueryRunner createDCQueryRunner(TestingPrestoServer hetuServer, Map<String, String> properties)
            throws Exception
    {
        hetuServer.installPlugin(new TpchPlugin());
        hetuServer.createCatalog("tpch", "tpch");
        hetuServer.installPlugin(new StateStoreManagerPlugin());
        hetuServer.loadStateSotre();

        DistributedQueryRunner distributedQueryRunner = null;
        try {
            distributedQueryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                    .setNodeCount(1)
                    .build();

            Map<String, String> connectorProperties = new HashMap<>(properties);
            connectorProperties.putIfAbsent("connection-url", hetuServer.getBaseUrl().toString());
            connectorProperties.putIfAbsent("connection-user", "root");
            distributedQueryRunner.installPlugin(new DataCenterPlugin());
            distributedQueryRunner.createDCCatalog("dc", "dc", connectorProperties);
            distributedQueryRunner.installPlugin(new TpchPlugin());
            distributedQueryRunner.createCatalog("tpch", "tpch", properties);

            return distributedQueryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, distributedQueryRunner);
            throw e;
        }
    }
}
