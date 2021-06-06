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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.relational.FunctionResolution;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.relational.Expressions.isNull;
import static java.util.Objects.requireNonNull;

public class SimplifyCountOverConstant
        implements Rule<AggregationNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(project().capturedAs(CHILD)));

    private final StandardFunctionResolution functionResolution;

    public SimplifyCountOverConstant(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        boolean changed = false;
        Map<Symbol, AggregationNode.Aggregation> aggregations = new LinkedHashMap<>(parent.getAggregations());

        for (Entry<Symbol, AggregationNode.Aggregation> entry : parent.getAggregations().entrySet()) {
            Symbol symbol = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();

            if (isCountOverConstant(aggregation, child.getAssignments())) {
                changed = true;
                aggregations.put(symbol, new AggregationNode.Aggregation(
                        new CallExpression(
                                "count",
                                functionResolution.countFunction(),
                                BIGINT,
                                ImmutableList.of(),
                                Optional.empty()),
                        ImmutableList.of(),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        aggregation.getMask()));
            }
        }

        if (!changed) {
            return Result.empty();
        }

        return Result.ofPlanNode(new AggregationNode(
                parent.getId(),
                child,
                aggregations,
                parent.getGroupingSets(),
                ImmutableList.of(),
                parent.getStep(),
                parent.getHashSymbol(),
                parent.getGroupIdSymbol(),
                parent.getAggregationType(),
                parent.getFinalizeSymbol()));
    }

    private boolean isCountOverConstant(AggregationNode.Aggregation aggregation, Assignments inputs)
    {
        if (!functionResolution.isCountFunction(aggregation.getFunctionHandle()) || aggregation.getArguments().size() != 1) {
            return false;
        }

        RowExpression argument = aggregation.getArguments().get(0);
        if (argument instanceof VariableReferenceExpression) {
            argument = inputs.get(new Symbol(((VariableReferenceExpression) argument).getName()));
        }

        return argument instanceof ConstantExpression && !isNull(argument);
    }
}
