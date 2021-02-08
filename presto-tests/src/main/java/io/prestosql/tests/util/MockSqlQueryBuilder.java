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
package io.prestosql.tests.util;

import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.builder.SqlQueryBuilder;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.TopNNode;

public class MockSqlQueryBuilder
        extends SqlQueryBuilder
{
    private int visits;

    public MockSqlQueryBuilder(Metadata metadata, Session session)
    {
        super(metadata, session);
    }

    @Override
    public String visitJoin(JoinNode node, SqlQueryBuilder.Context context)
    {
        visits++;
        return super.visitJoin(node, context);
    }

    @Override
    public String visitProject(ProjectNode node, SqlQueryBuilder.Context context)
    {
        visits++;
        return super.visitProject(node, context);
    }

    @Override
    public String visitAggregation(AggregationNode node, SqlQueryBuilder.Context context)
    {
        visits++;
        return super.visitAggregation(node, context);
    }

    @Override
    public String visitSort(SortNode node, SqlQueryBuilder.Context context)
    {
        visits++;
        return super.visitSort(node, context);
    }

    @Override
    public String visitTopN(TopNNode node, SqlQueryBuilder.Context context)
    {
        visits++;
        return super.visitTopN(node, context);
    }

    @Override
    public String visitFilter(FilterNode node, SqlQueryBuilder.Context context)
    {
        visits++;
        return super.visitFilter(node, context);
    }

    @Override
    public String visitLimit(LimitNode node, SqlQueryBuilder.Context context)
    {
        visits++;
        return super.visitLimit(node, context);
    }

    @Override
    public String visitTableScan(TableScanNode node, SqlQueryBuilder.Context context)
    {
        visits++;
        return super.visitTableScan(node, context);
    }

    public int getVisits()
    {
        return visits;
    }
}
