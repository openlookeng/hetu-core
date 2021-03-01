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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.RowExpressionVariableInliner;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.VariablesExtractor;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class TableDeleteOperator
        implements Operator
{
    public static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private final TypeProvider types;
    private final Map<Symbol, ColumnHandle> assignments;

    public static class TableDeleteOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Metadata metadata;
        private final Session session;
        private final TableHandle tableHandle;
        private final Optional<RowExpression> filter;
        private final Optional<Map<Symbol, Integer>> sourceLayout;
        private final TypeProvider types;
        private final Map<Symbol, ColumnHandle> assignments;
        private boolean closed;

        public TableDeleteOperatorFactory(int operatorId, PlanNodeId planNodeId, Metadata metadata, Session session, TableHandle tableHandle,
                Optional<Map<Symbol, Integer>> sourceLayout, Optional<RowExpression> filter, Map<Symbol, ColumnHandle> assignments, TypeProvider types)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.sourceLayout = sourceLayout;
            this.filter = requireNonNull(filter, "filter is null");
            this.types = types;
            this.assignments = assignments;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableDeleteOperator.class.getSimpleName());
            return new TableDeleteOperator(context, metadata, session, tableHandle, sourceLayout, filter, types, assignments);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TableDeleteOperatorFactory(operatorId, planNodeId, metadata, session, tableHandle, sourceLayout, filter, assignments, types);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final Metadata metadata;
    private final Session session;
    private final TableHandle tableHandle;
    private final Optional<Map<Symbol, Integer>> sourceLayout;
    private final Map<Symbol, RowExpression> symbolExpressionMap;
    private final Optional<RowExpression> filter;

    private State state = State.RUNNING;

    public TableDeleteOperator(OperatorContext operatorContext, Metadata metadata, Session session, TableHandle tableHandle,
            Optional<Map<Symbol, Integer>> sourceLayout, Optional<RowExpression> filter, TypeProvider types, Map<Symbol, ColumnHandle> assignments)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.sourceLayout = sourceLayout;
        this.filter = requireNonNull(filter, "filter is null");
        this.symbolExpressionMap = new HashMap<>();
        this.types = types;
        this.assignments = assignments;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (state == State.RUNNING) {
            state = State.FINISHING;
        }
    }

    @Override
    public boolean isFinished()
    {
        if (state == State.FINISHED) {
            return true;
        }
        return false;
    }

    @Override
    public boolean needsInput()
    {
        if (!sourceLayout.isPresent()) {
            //Input not expected without any sources.
            return false;
        }
        if (state == State.RUNNING) {
            return true;
        }
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        if (!sourceLayout.isPresent()) {
            throw new UnsupportedOperationException();
        }
        requireNonNull(page, "page is null");
        checkState(state == State.RUNNING, "Operator is %s", state);
        LiteralEncoder literalEncoder = new LiteralEncoder(metadata);
        //Convert the incoming data as Literal values for symbols
        //Current impl, one symbol->one value is expected.
        sourceLayout.get().keySet().stream().forEach(symbol -> {
            Block block = page.getBlock(sourceLayout.get().get(symbol));
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (!block.isNull(i)) {
                    Type type = types.get(symbol);
                    symbolExpressionMap.putIfAbsent(symbol, ConstantExpression.createConstantExpression(block, type));
                }
            }
        });
    }

    @Override
    public Page getOutput()
    {
        if (state == State.FINISHED || (sourceLayout.isPresent() && state != State.FINISHING)) {
            return null;
        }
        state = State.FINISHED;

        TableHandle tableHandleForDelete = tableHandle;
        if (sourceLayout.isPresent()) {
            //Replace the values from subqueries in filter predicates
            VariablesExtractor.extractUnique(filter.get()).stream().forEach(expr -> symbolExpressionMap.putIfAbsent(new Symbol(expr.getName()), expr));
            RowExpression rowExpression = RowExpressionVariableInliner.inlineVariables(node -> symbolExpressionMap.get(new Symbol(node.getName())), filter.get());

            //Create the tuple domain based on filter and values from source;
            RowExpressionDomainTranslator.ExtractionResult<VariableReferenceExpression> decomposedPredicate = (new RowExpressionDomainTranslator(metadata)).fromPredicate(
                    session.toConnectorSession(),
                    rowExpression,
                    RowExpressionDomainTranslator.BASIC_COLUMN_EXTRACTOR);
            TupleDomain<ColumnHandle> tupleDomain = decomposedPredicate.getTupleDomain().transform(variableName -> assignments.get(new Symbol(variableName.getName())));
            Constraint constraint = new Constraint(tupleDomain);

            //Apply the constraint on the table handle
            Optional<TableHandle> tableHandle = metadata.applyDelete(session, this.tableHandle, constraint);
            if (tableHandle.isPresent()) {
                tableHandleForDelete = tableHandle.get();
            }
        }
        OptionalLong rowsDeletedCount = metadata.executeDelete(session, tableHandleForDelete);

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder page = new PageBuilder(1, TYPES);
        BlockBuilder rowsBuilder = page.getBlockBuilder(0);
        page.declarePosition();
        if (rowsDeletedCount.isPresent()) {
            BIGINT.writeLong(rowsBuilder, rowsDeletedCount.getAsLong());
        }
        else {
            rowsBuilder.appendNull();
        }
        return page.build();
    }
}
