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

import io.hetu.core.spi.cube.CubeFilter;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeMetadataBuilder;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.Session;
import io.prestosql.cube.CubeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.Page;
import io.prestosql.spi.cube.CubeUpdateMetadata;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Expression;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.hetu.core.spi.cube.CubeStatus.READY;
import static io.prestosql.cube.CubeManager.STAR_TREE;
import static java.util.Objects.requireNonNull;

// create cube statement does not have snapshot support
@RestorableConfig(unsupported = true)
public class CubeFinishOperator
        implements Operator
{
    public static class CubeFinishOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Session session;
        private final CubeManager cubeManager;
        private final CubeUpdateMetadata cubeUpdateMetadata;
        private final Metadata metadata;
        private final TypeProvider types;
        private boolean closed;

        public CubeFinishOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Session session,
                Metadata metadata,
                TypeProvider types,
                CubeManager cubeManager,
                CubeUpdateMetadata cubeUpdateMetadata)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.types = requireNonNull(types, "types is null");
            this.cubeManager = requireNonNull(cubeManager, "cubeManager is null");
            this.cubeUpdateMetadata = requireNonNull(cubeUpdateMetadata, "cubeUpdateMetadata is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, CubeFinishOperator.class.getSimpleName());
            return new CubeFinishOperator(context, cubeManager, cubeUpdateMetadata, types, metadata, session);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new CubeFinishOperatorFactory(operatorId, planNodeId, session, metadata, types, cubeManager, cubeUpdateMetadata);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final CubeMetaStore cubeMetastore;
    private final CubeUpdateMetadata updateMetadata;
    private final TypeProvider types;
    private final Metadata metadata;
    private final Session session;
    private State state = State.NEEDS_INPUT;
    private Page page;

    public CubeFinishOperator(
            OperatorContext operatorContext,
            CubeManager cubeManager,
            CubeUpdateMetadata updateMetadata,
            TypeProvider types,
            Metadata metadata,
            Session session)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.cubeMetastore = cubeManager.getMetaStore(STAR_TREE).get();
        this.updateMetadata = updateMetadata;
        this.types = types;
        this.metadata = metadata;
        this.session = session;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkState(this.page == null, "page is already set.");
        this.page = page;
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }
        synchronized (cubeMetastore) {
            //handle concurrent inserts into cube.
            CubeMetadata cubeMetadata = cubeMetastore.getMetadataFromCubeName(updateMetadata.getCubeName()).get();
            CubeMetadataBuilder builder = cubeMetastore.getBuilder(cubeMetadata);
            builder.withCubeFilter(mergePredicates(cubeMetadata.getCubeFilter(), updateMetadata.getDataPredicateString()));
            builder.setTableLastUpdatedTime(updateMetadata.getTableLastUpdatedTime());
            builder.setCubeLastUpdatedTime(System.currentTimeMillis());
            builder.setCubeStatus(READY);
            cubeMetastore.persist(builder.build());
            state = State.FINISHED;
        }
        return page;
    }

    private CubeFilter mergePredicates(CubeFilter existing, String newPredicateString)
    {
        String sourceTablePredicate = existing == null ? null : existing.getSourceTablePredicate();
        if (newPredicateString == null && sourceTablePredicate == null) {
            return null;
        }
        else if (newPredicateString == null) {
            return new CubeFilter(sourceTablePredicate, null);
        }
        SqlParser sqlParser = new SqlParser();
        Expression newPredicate = sqlParser.createExpression(newPredicateString, new ParsingOptions());
        if (!updateMetadata.isOverwrite() && existing != null && existing.getCubePredicate() != null) {
            newPredicate = ExpressionUtils.or(sqlParser.createExpression(existing.getCubePredicate(), new ParsingOptions()), newPredicate);
        }
        //Merge new data predicate with existing predicate string
        CubeRangeCanonicalizer canonicalizer = new CubeRangeCanonicalizer(metadata, session, types);
        newPredicate = canonicalizer.mergePredicates(newPredicate);
        String formatExpression = newPredicate.equals(BooleanLiteral.TRUE_LITERAL) ? null : ExpressionFormatter.formatExpression(newPredicate, Optional.empty());
        return formatExpression == null && sourceTablePredicate == null ? null : new CubeFilter(sourceTablePredicate, formatExpression);
    }

    @Override
    public Page pollMarker()
    {
        return null;
    }

    @Override
    public void finish()
    {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }
}
