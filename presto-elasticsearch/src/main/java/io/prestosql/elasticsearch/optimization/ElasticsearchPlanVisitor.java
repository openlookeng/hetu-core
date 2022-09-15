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
import io.prestosql.elasticsearch.ElasticsearchTableHandle;
import io.prestosql.spi.SymbolAllocator;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.PlanVisitor;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class ElasticsearchPlanVisitor
        extends PlanVisitor<PlanNode, Void>
{
    private final PlanNodeIdAllocator idAllocator;
    private final ConnectorSession session;
    private final Map<String, Type> types;
    private final SymbolAllocator symbolAllocator;

    private final ElasticSearchRowExpressionConverter rowExpressionConverter;

    @Inject
    public ElasticsearchPlanVisitor(PlanNodeIdAllocator idAllocator, ConnectorSession session, Map<String, Type> types, SymbolAllocator symbolAllocator, ElasticSearchRowExpressionConverter elasticSearchRowExpressionConverter)
    {
        this.idAllocator = idAllocator;
        this.session = session;
        this.types = types;
        this.symbolAllocator = symbolAllocator;
        this.rowExpressionConverter = elasticSearchRowExpressionConverter;
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
    public PlanNode visitPlan(PlanNode node, Void context)
    {
        Optional<PlanNode> pushDownPlan = tryCreatingNewScanNode(node);
        return pushDownPlan.orElseGet(() -> replaceChildren(
                node, node.getSources().stream().map(source -> source.accept(this, null)).collect(toImmutableList())));
    }

    private Optional<PlanNode> tryCreatingNewScanNode(PlanNode node)
    {
        if (node instanceof FilterNode) {
            return tryCreatingNewFilterNode(((FilterNode) node));
        }
        else if (node instanceof AggregationNode) {
            // TODO: 9/2/2022 implement specific logic when developing pushdown for aggregate
            return Optional.empty();
        }
        return Optional.empty();
    }

    private Optional<PlanNode> tryCreatingNewFilterNode(FilterNode node)
    {
        RowExpression predicate = node.getPredicate();
        Optional<String> esQuery = convertPredicateToESQuery(predicate);
        if (!esQuery.isPresent()) {
            return Optional.empty();
        }

        TableScanNode tableScanNodeOriginal = (TableScanNode) node.getSource();
        TableHandle tableHandleOriginal = tableScanNodeOriginal.getTable();
        ElasticsearchTableHandle connectorHandle = (ElasticsearchTableHandle) tableHandleOriginal.getConnectorHandle();

        ElasticsearchTableHandle connectorHandleNew = new ElasticsearchTableHandle(connectorHandle.getSchema(), connectorHandle.getIndex(), esQuery);
        TableHandle tableHandleNew = new TableHandle(tableHandleOriginal.getCatalogName(), connectorHandleNew, tableHandleOriginal.getTransaction(), tableHandleOriginal.getLayout());
        TableScanNode tableScanNodeNew = new TableScanNode(idAllocator.getNextId(), tableHandleNew, tableScanNodeOriginal.getOutputSymbols(), tableScanNodeOriginal.getAssignments(), tableScanNodeOriginal.getEnforcedConstraint(), Optional.of(predicate), tableScanNodeOriginal.getStrategy(), tableScanNodeOriginal.getReuseTableScanMappingId(), tableScanNodeOriginal.getConsumerTableScanNodeCount(), tableScanNodeOriginal.isForDelete());
        FilterNode filterNodeNew = new FilterNode(idAllocator.getNextId(), tableScanNodeNew, predicate);

        return Optional.of(filterNodeNew);
    }

    private Optional<String> convertPredicateToESQuery(RowExpression predicate)
    {
        ElasticSearchConverterContext converterContext = new ElasticSearchConverterContext();
        String queryString = predicate.accept(this.rowExpressionConverter, converterContext);
        if (converterContext.isHasConversionFailed()) {
            return Optional.empty();
        }
        else {
            return Optional.of(queryString);
        }
    }
}
