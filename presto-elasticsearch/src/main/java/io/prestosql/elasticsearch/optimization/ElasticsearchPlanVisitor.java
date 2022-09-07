package io.prestosql.elasticsearch.optimization;

import com.google.inject.Inject;
import io.prestosql.elasticsearch.ElasticsearchTableHandle;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.*;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

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

    private static PlanNode replaceChildren(PlanNode node, List<PlanNode> children)
    {
        List<PlanNode> childrenNodes = node.getSources();
        for (int i = 0; i < childrenNodes.size(); i++) {
            if (children.get(i) != childrenNodes.get(i)) {
                return node.replaceChildren(children);
            }
        }
        return node;
    }


    @Override
    public PlanNode visitPlan(PlanNode node, Void context) {
        Optional<PlanNode> pushDownPlan = tryCreatingNewScanNode(node);
        return pushDownPlan.orElseGet(() -> replaceChildren(
                node, node.getSources().stream().map(source -> source.accept(this, null)).collect(toImmutableList())));
    }

    private Optional<PlanNode> tryCreatingNewScanNode(PlanNode node) {
        if (node instanceof FilterNode) {
            return tryCreatingNewFilterNode(((FilterNode) node));
        } else if (node instanceof AggregationNode) {
            // TODO: 9/2/2022 implement specific logic when developing pushdown for aggregate
            return Optional.empty();
        }
        return Optional.empty();
    }

    private Optional<PlanNode> tryCreatingNewFilterNode(FilterNode node) {
        RowExpression predicate = node.getPredicate();
        Optional<String> esQuery = convertPredicateToESQuery(predicate);
        if (!esQuery.isPresent()) {
            return Optional.empty();
        }

        TableScanNode tableScanNodeOriginal = (TableScanNode) node.getSource();
        TableHandle tableHandleOriginal = tableScanNodeOriginal.getTable();
        ElasticsearchTableHandle connectorHandle = (ElasticsearchTableHandle)tableHandleOriginal.getConnectorHandle();
        TableScanNode tableScanNodeNew = new TableScanNode(idAllocator.getNextId(), tableHandleOriginal, tableScanNodeOriginal.getOutputSymbols(), tableScanNodeOriginal.getAssignments(), tableScanNodeOriginal.getEnforcedConstraint(), Optional.of(predicate), tableScanNodeOriginal.getStrategy(), tableScanNodeOriginal.getReuseTableScanMappingId(), tableScanNodeOriginal.getConsumerTableScanNodeCount(), tableScanNodeOriginal.isForDelete());
        FilterNode filterNodeNew = new FilterNode(idAllocator.getNextId(), tableScanNodeNew, predicate);
        return Optional.of(filterNodeNew);
    }

    private Optional<String> convertPredicateToESQuery(RowExpression predicate) {
        return null;
    }
}
