package io.prestosql.elasticsearch.optimization;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.connector.ConnectorPlanOptimizerProvider;

import java.util.Set;

public class ElasticSearchPlanOptimizerProvider implements ConnectorPlanOptimizerProvider {

    private ElasticSearchPlanOptimizer elasticSearchPlanOptimizer;

    @Inject
    public ElasticSearchPlanOptimizerProvider(ElasticSearchPlanOptimizer elasticSearchPlanOptimizer) {
        this.elasticSearchPlanOptimizer = elasticSearchPlanOptimizer;
    }

    @Override
    public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers() {
        return ImmutableSet.of(elasticSearchPlanOptimizer);
    }

    @Override
    public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers() {
        return ImmutableSet.of();
    }
}
