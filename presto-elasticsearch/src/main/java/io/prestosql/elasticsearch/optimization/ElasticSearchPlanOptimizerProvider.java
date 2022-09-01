package io.prestosql.elasticsearch.optimization;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.ConnectorPlanOptimizer;
import io.prestosql.spi.connector.ConnectorPlanOptimizerProvider;

import java.util.Set;

public class ElasticSearchPlanOptimizerProvider implements ConnectorPlanOptimizerProvider {
    @Override
    public Set<ConnectorPlanOptimizer> getLogicalPlanOptimizers() {
        return ImmutableSet.of();
    }

    @Override
    public Set<ConnectorPlanOptimizer> getPhysicalPlanOptimizers() {
        return ImmutableSet.of();
    }
}
