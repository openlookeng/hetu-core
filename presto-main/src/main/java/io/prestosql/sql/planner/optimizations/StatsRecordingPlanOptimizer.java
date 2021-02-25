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
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.sql.planner.OptimizerStatsRecorder;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;

import static java.util.Objects.requireNonNull;

public final class StatsRecordingPlanOptimizer
        implements PlanOptimizer
{
    private final OptimizerStatsRecorder stats;
    private final PlanOptimizer delegate;

    public StatsRecordingPlanOptimizer(OptimizerStatsRecorder stats, PlanOptimizer delegate)
    {
        this.stats = requireNonNull(stats, "stats is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        stats.register(delegate);
    }

    public final PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            PlanSymbolAllocator planSymbolAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        PlanNode result;
        long duration;
        try {
            long start = System.nanoTime();
            result = delegate.optimize(plan, session, types, planSymbolAllocator, idAllocator, warningCollector);
            duration = System.nanoTime() - start;
        }
        catch (RuntimeException e) {
            stats.recordFailure(delegate);
            throw e;
        }
        stats.record(delegate, duration);
        return result;
    }
}
