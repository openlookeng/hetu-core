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

import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static io.prestosql.sql.DynamicFilters.extractDynamicFiltersAsUnion;
import static io.prestosql.sql.DynamicFilters.getDescriptor;

/**
 * When dynamic filter assignments are present on a Join node, they should be consumed by a Filter node on it's probe side
 */
public class DynamicFiltersChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, TypeAnalyzer typeAnalyzer, TypeProvider types, WarningCollector warningCollector)
    {
        plan.accept(new InternalPlanVisitor<Set<String>, List<String>>()
        {
            @Override
            public Set<String> visitPlan(PlanNode node, List<String> context)
            {
                Set<String> consumed = new HashSet<>();
                for (PlanNode source : node.getSources()) {
                    consumed.addAll(source.accept(this, context));
                }
                return consumed;
            }

            @Override
            public Set<String> visitOutput(OutputNode node, List<String> context)
            {
                Set<String> unmatched = visitPlan(node, context);
                verify(unmatched.isEmpty(), "All consumed dynamic filters could not be matched with a join/semi-join.");
                return unmatched;
            }

            @Override
            public Set<String> visitJoin(JoinNode node, List<String> context)
            {
                Set<String> currentJoinDynamicFilters = node.getDynamicFilters().keySet();
                context.addAll(currentJoinDynamicFilters);
                Set<String> consumedProbeSide = node.getLeft().accept(this, context);
                verify(difference(currentJoinDynamicFilters, consumedProbeSide).isEmpty(),
                        "Dynamic filters present in join were not fully consumed by it's probe side.");

                context.removeAll(currentJoinDynamicFilters);
                Set<String> consumedBuildSide = node.getRight().accept(this, context);
                verify(intersection(currentJoinDynamicFilters, consumedBuildSide).isEmpty());

                Set<String> unmatched = new HashSet<>(consumedBuildSide);
                unmatched.addAll(consumedProbeSide);
                unmatched.removeAll(currentJoinDynamicFilters);
                return ImmutableSet.copyOf(unmatched);
            }

            @Override
            public Set<String> visitSemiJoin(SemiJoinNode node, List<String> context)
            {
                if (node.getDynamicFilterId().isPresent()) {
                    Set<String> currentJoinDynamicFilters = ImmutableSet.of(node.getDynamicFilterId().get());
                    context.addAll(currentJoinDynamicFilters);
                }
                Set<String> consumedSourceSide = node.getSource().accept(this, context);
                if (node.getDynamicFilterId().isPresent()) {
                    context.remove(node.getDynamicFilterId().get());
                }
                Set<String> consumedFilteringSourceSide = node.getFilteringSource().accept(this, context);

                Set<String> unmatched = new HashSet<>(consumedSourceSide);
                unmatched.addAll(consumedFilteringSourceSide);

                if (node.getDynamicFilterId().isPresent()) {
                    String dynamicFilterId = node.getDynamicFilterId().get();
                    verify(consumedSourceSide.contains(dynamicFilterId),
                            "The dynamic filter %s present in semi-join was not consumed by it's source side.", dynamicFilterId);
                    verify(!consumedFilteringSourceSide.contains(dynamicFilterId),
                            "The dynamic filter %s present in semi-join was consumed by it's filtering source side.", dynamicFilterId);
                    unmatched.remove(dynamicFilterId);
                }

                return ImmutableSet.copyOf(unmatched);
            }

            @Override
            public Set<String> visitFilter(FilterNode node, List<String> context)
            {
                ImmutableSet.Builder<String> consumed = ImmutableSet.builder();
                List<RowExpression> predicates = LogicalRowExpressions.extractAllPredicates(node.getPredicate());

                List<List<DynamicFilters.Descriptor>> dynamicFilters = extractDynamicFiltersAsUnion(Optional.of(node.getPredicate()), metadata);
                if (dynamicFilters.size() > 1) {
                    for (List<DynamicFilters.Descriptor> descriptorList : dynamicFilters) {
                        List<String> dynamicFilterIds = descriptorList.stream().map(desc -> desc.getId()).collect(Collectors.toList());
                        if (context.containsAll(dynamicFilterIds)) {
                            consumed.addAll(dynamicFilterIds);
                        }
                    }
                }
                else {
                    predicates.stream().filter(exp -> getDescriptor(exp).isPresent())
                            .map(exp -> getDescriptor(exp).get().getId())
                            .forEach(consumed::add);
                }

                consumed.addAll(node.getSource().accept(this, context));
                return consumed.build();
            }
        }, new ArrayList<>());
    }
}
