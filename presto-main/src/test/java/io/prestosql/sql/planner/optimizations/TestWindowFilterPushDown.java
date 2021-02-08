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
package io.prestosql.sql.planner.optimizations;

import io.prestosql.Session;
import io.prestosql.operator.window.RankingFunction;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.plan.TopNRankingNumberNode;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.prestosql.SystemSessionProperties.OPTIMIZE_TOP_N_RANKING_NUMBER;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyNot;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.limit;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.node;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static java.lang.String.format;

public class TestWindowFilterPushDown
        extends BasePlanTest
{
    @Test
    public void testLimitAboveWindow()
    {
        String sqlFormat = "SELECT " +
                "%s() OVER (PARTITION BY suppkey ORDER BY orderkey) partition_number FROM lineitem LIMIT 10";

        for (RankingFunction rankingFunction : RankingFunction.values()) {
            @Language("SQL") String sql = format(sqlFormat, rankingFunction.getValue().getName());

            assertPlanWithSession(
                    sql,
                    optimizeTopNRankingNumber(true),
                    true,
                    anyTree(
                            limit(10, anyTree(
                                    node(TopNRankingNumberNode.class,
                                            anyTree(
                                                    tableScan("lineitem")))))));

            assertPlanWithSession(
                    sql,
                    optimizeTopNRankingNumber(false),
                    true,
                    anyTree(
                            limit(10, anyTree(
                                    node(WindowNode.class,
                                            anyTree(
                                                    tableScan("lineitem")))))));
        }
    }

    @Test
    public void testFilterAboveWindow()
    {
        String sqlFormat = "SELECT * FROM " +
                "(SELECT %s() OVER (PARTITION BY suppkey ORDER BY orderkey) partition_row_number FROM lineitem) " +
                "WHERE partition_row_number < 10";

        for (RankingFunction rankingFunction : RankingFunction.values()) {
            @Language("SQL") String sql = format(sqlFormat, rankingFunction.getValue().getName());
            assertPlanWithSession(
                    sql,
                    optimizeTopNRankingNumber(true),
                    true,
                    anyTree(
                            anyNot(FilterNode.class,
                                    node(TopNRankingNumberNode.class,
                                            anyTree(
                                                    tableScan("lineitem"))))));

            assertPlanWithSession(
                    sql,
                    optimizeTopNRankingNumber(false),
                    true,
                    anyTree(
                            node(FilterNode.class,
                                    anyTree(
                                            node(WindowNode.class,
                                                    anyTree(
                                                            tableScan("lineitem")))))));
        }
    }

    private Session optimizeTopNRankingNumber(boolean enabled)
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZE_TOP_N_RANKING_NUMBER, Boolean.toString(enabled))
                .build();
    }
}
