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
package io.prestosql.sql.planner.iterative;

import io.prestosql.Session;
import io.prestosql.cost.CostProvider;
import io.prestosql.cost.StatsProvider;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.utils.OptimizerUtils;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface Rule<T>
{
    /**
     * Returns a pattern to which plan nodes this rule applies.
     */
    Pattern<T> getPattern();

    default boolean isEnabled(Session session)
    {
        return OptimizerUtils.isEnabledRule(this, session);
    }

    Result apply(T node, Captures captures, Context context);

    interface Context
    {
        Lookup getLookup();

        PlanNodeIdAllocator getIdAllocator();

        PlanSymbolAllocator getSymbolAllocator();

        Session getSession();

        StatsProvider getStatsProvider();

        CostProvider getCostProvider();

        void checkTimeoutNotExhausted();

        WarningCollector getWarningCollector();
    }

    final class Result
    {
        public static Result empty()
        {
            return new Result(Optional.empty());
        }

        public static Result ofPlanNode(PlanNode transformedPlan)
        {
            return new Result(Optional.of(transformedPlan));
        }

        private final Optional<PlanNode> transformedPlan;

        private Result(Optional<PlanNode> transformedPlan)
        {
            this.transformedPlan = requireNonNull(transformedPlan, "transformedPlan is null");
        }

        public Optional<PlanNode> getTransformedPlan()
        {
            return transformedPlan;
        }

        public boolean isEmpty()
        {
            return !transformedPlan.isPresent();
        }
    }
}
