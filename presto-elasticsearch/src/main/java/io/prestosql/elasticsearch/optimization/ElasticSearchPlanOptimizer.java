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
