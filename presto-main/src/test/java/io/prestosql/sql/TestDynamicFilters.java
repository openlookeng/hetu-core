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
package io.prestosql.sql;

import com.google.common.collect.ImmutableList;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.util.Arrays.asList;

public class TestDynamicFilters
{
    private Metadata metadata;

    @BeforeClass
    public void setup()
    {
        metadata = createTestMetadataManager();
    }

    @Test
    public void testExtractStaticFilters()
    {
        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata),
                new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
        ConstantExpression staticExpression = new ConstantExpression(utf8Slice("age"), VarcharType.VARCHAR);
        VariableReferenceExpression variableExpression = new VariableReferenceExpression("col", VarcharType.VARCHAR);
        FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupFunction("rank", ImmutableList.of());
        RowExpression dynamicFilterExpression = call(DynamicFilters.Function.NAME, functionHandle, staticExpression.getType(), asList(staticExpression, variableExpression), Optional.empty());

        RowExpression combinedExpression = logicalRowExpressions.combineConjuncts(staticExpression, dynamicFilterExpression);
        Optional<RowExpression> extractedExpression = DynamicFilters.extractStaticFilters(Optional.of(combinedExpression), metadata);
        assertEquals(extractedExpression.get(), staticExpression);

        RowExpression combinedStaticExpression = logicalRowExpressions.combineConjuncts(staticExpression, variableExpression);
        combinedExpression = logicalRowExpressions.combineConjuncts(staticExpression, variableExpression, dynamicFilterExpression);
        extractedExpression = DynamicFilters.extractStaticFilters(Optional.of(combinedExpression), metadata);
        assertEquals(extractedExpression.get(), combinedStaticExpression);
    }

    @Test
    public void testExtractDynamicFilterExpression()
    {
        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata),
                new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
        ConstantExpression staticExpression1 = new ConstantExpression(utf8Slice("age"), VarcharType.VARCHAR);
        VariableReferenceExpression variableExpression1 = new VariableReferenceExpression("col", VarcharType.VARCHAR);
        ConstantExpression staticExpression2 = new ConstantExpression(utf8Slice("age"), VarcharType.VARCHAR);
        VariableReferenceExpression variableExpression2 = new VariableReferenceExpression("col", VarcharType.VARCHAR);
        FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupFunction("rank", ImmutableList.of());
        RowExpression dynamicFilterExpression1 = call(DynamicFilters.Function.NAME, functionHandle, staticExpression1.getType(), asList(staticExpression1, variableExpression1), Optional.empty());
        RowExpression dynamicFilterExpression2 = call(DynamicFilters.Function.NAME, functionHandle, staticExpression2.getType(), asList(staticExpression2, variableExpression2), Optional.empty());

        RowExpression combineDynamicFilterExpression = logicalRowExpressions.combineDisjuncts(asList(dynamicFilterExpression1, dynamicFilterExpression2));
        RowExpression combinedExpression = logicalRowExpressions.combineConjuncts(staticExpression1, combineDynamicFilterExpression);
        RowExpression extractedExpression = DynamicFilters.extractDynamicFilterExpression(combinedExpression, metadata);
        assertEquals(extractedExpression, combineDynamicFilterExpression);
    }

    @Test
    public void testExtractDynamicFiltersAsUnion()
    {
        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata),
                new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
        ConstantExpression staticExpression1 = new ConstantExpression(utf8Slice("age"), VarcharType.VARCHAR);
        VariableReferenceExpression variableExpression1 = new VariableReferenceExpression("col1", VarcharType.VARCHAR);
        ConstantExpression staticExpression2 = new ConstantExpression(utf8Slice("id"), VarcharType.VARCHAR);
        VariableReferenceExpression variableExpression2 = new VariableReferenceExpression("col2", VarcharType.VARCHAR);
        FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupFunction("rank", ImmutableList.of());
        RowExpression dynamicFilterExpression1 = call(DynamicFilters.Function.NAME, functionHandle, staticExpression1.getType(), asList(staticExpression1, variableExpression1), Optional.empty());
        RowExpression dynamicFilterExpression2 = call(DynamicFilters.Function.NAME, functionHandle, staticExpression2.getType(), asList(staticExpression2, variableExpression2), Optional.empty());
        List<DynamicFilters.Descriptor> dynamicFilter1 = new ArrayList<>();
        dynamicFilter1.add(new DynamicFilters.Descriptor("age", variableExpression1, Optional.empty()));
        List<DynamicFilters.Descriptor> dynamicFilter2 = new ArrayList<>();
        dynamicFilter2.add(new DynamicFilters.Descriptor("id", variableExpression2, Optional.empty()));
        List<List<DynamicFilters.Descriptor>> dynamicFilterUnion = new ArrayList<>();
        dynamicFilterUnion.add(dynamicFilter1);
        dynamicFilterUnion.add(dynamicFilter2);

        RowExpression combineDynamicFilterExpression = logicalRowExpressions.combineDisjuncts(asList(dynamicFilterExpression1, dynamicFilterExpression2));
        RowExpression combinedExpression = logicalRowExpressions.combineConjuncts(staticExpression1, combineDynamicFilterExpression);
        List<List<DynamicFilters.Descriptor>> extractedExpression = DynamicFilters.extractDynamicFiltersAsUnion(Optional.of(combinedExpression), metadata);
        assertEquals(extractedExpression, dynamicFilterUnion);
    }
}
