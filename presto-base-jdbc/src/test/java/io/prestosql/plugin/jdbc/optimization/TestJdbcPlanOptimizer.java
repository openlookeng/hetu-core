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
package io.prestosql.plugin.jdbc.optimization;

import com.google.common.collect.ImmutableMap;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.relational.ConnectorRowExpressionService;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertTrue;

public class TestJdbcPlanOptimizer
        extends TestBaseJdbcPushDownBase
{
    private static final SessionHolder defaultSessionHolder = new SessionHolder();
    private static final JdbcTableHandle jdbcTable = testTable;

    private void matchPlan(PlanNode optimizedPlan, String exceptedSql)
    {
        assertTrue(optimizedPlan instanceof ProjectNode
                && ((ProjectNode) optimizedPlan).getSource() instanceof TableScanNode
                && ((JdbcTableHandle) ((TableScanNode) ((ProjectNode) optimizedPlan).getSource()).getTable().getConnectorHandle()).getGeneratedSql().map(JdbcQueryGeneratorResult.GeneratedSql::getSql).get().equals(exceptedSql));
    }

    @Test
    public void testLimitPushDownWithStarSelection()
    {
        PlanBuilder pb = createPlanBuilder();
        PlanNode originalPlan = limit(pb, 50L, tableScan(pb, jdbcTable, regionId, city, fare, amount));
        PlanNode optimized = getOptimizedPlan(pb, originalPlan);
        matchPlan(optimized, "SELECT regionid, city, fare, amount FROM 'table' LIMIT 50");
    }

    private PlanNode getOptimizedPlan(PlanBuilder planBuilder, PlanNode originalPlan)
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        JdbcClient client = new TestPushwonClient();
        Metadata metadata = MetadataManager.createTestMetadataManager();
        RowExpressionService rowExpressionService = new ConnectorRowExpressionService(new RowExpressionDomainTranslator(metadata), new RowExpressionDeterminismEvaluator(metadata));
        JdbcPlanOptimizer optimizer = new JdbcPlanOptimizer(client, new TestTypeManager(), config, rowExpressionService);
        return optimizer.optimize(
                originalPlan,
                defaultSessionHolder.getConnectorSession(),
                ImmutableMap.<String, Type>builder()
                        .put("regionid", INTEGER)
                        .put("city", VARCHAR)
                        .put("fare", DOUBLE)
                        .put("amount", BIGINT)
                        .build(),
                new PlanSymbolAllocator(),
                planBuilder.getIdAllocator());
    }
}
