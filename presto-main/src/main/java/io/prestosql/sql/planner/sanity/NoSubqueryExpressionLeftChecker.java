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
package io.prestosql.sql.planner.sanity;

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.sql.planner.ExpressionExtractor;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.DefaultTraversalVisitor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SubqueryExpression;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public final class NoSubqueryExpressionLeftChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
    {
        List<Expression> expressions = ExpressionExtractor.extractExpressions(plan)
                .stream()
                .filter(OriginalExpressionUtils::isExpression)
                .map(OriginalExpressionUtils::castToExpression)
                .collect(toImmutableList());
        for (Expression expression : expressions) {
            new DefaultTraversalVisitor<Void, Void>()
            {
                @Override
                protected Void visitSubqueryExpression(SubqueryExpression node, Void context)
                {
                    throw new IllegalStateException(String.format("Unexpected subquery expression in logical plan: %s", node));
                }
            }.process(expression, null);
        }
    }
}
