package io.prestosql.elasticsearch.optimization;

import com.google.inject.Inject;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.PlanVisitor;
import io.prestosql.spi.type.Type;

import java.util.Map;

public class ElasticsearchPlanVisitor extends PlanVisitor<PlanNode, Void> {
    private final PlanNodeIdAllocator idAllocator;
    private final ConnectorSession session;
    private final Map<String, Type> types;
    private final SymbolAllocator symbolAllocator;

    @Inject
    public ElasticsearchPlanVisitor(PlanNodeIdAllocator idAllocator, ConnectorSession session, Map<String, Type> types, SymbolAllocator symbolAllocator) {
        this.idAllocator = idAllocator;
        this.session = session;
        this.types = types;
        this.symbolAllocator = symbolAllocator;
    }

    @Override
    public PlanNode visitPlan(PlanNode node, Void context) {
        return null;
    }
}
