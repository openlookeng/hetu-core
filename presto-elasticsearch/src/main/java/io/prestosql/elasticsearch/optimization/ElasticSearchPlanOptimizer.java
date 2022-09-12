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
package io.prestosql.elasticsearch.optimization;

import com.google.inject.Inject;
import io.prestosql.elasticsearch.ElasticsearchConfig;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.type.Type;

import java.util.Map;

public class ElasticSearchPlanOptimizer implements ConnectorPlanOptimizer {

    private final ElasticsearchConfig elasticsearchConfig;

    @Inject
    public ElasticSearchPlanOptimizer(ElasticsearchConfig elasticsearchConfig) {
        this.elasticsearchConfig = elasticsearchConfig;
    }

    @Override
    public PlanNode optimize(PlanNode maxSubPlan, ConnectorSession session, Map<String, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator) {
        if (!elasticsearchConfig.isPushDownEnabled()) {
            return maxSubPlan;
        }
        return maxSubPlan.accept(new ElasticsearchPlanVisitor(idAllocator,session,types, symbolAllocator, new ElasticSearchRowExpressionConverter()), null);
    }
}
