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

import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeMetadataBuilder;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.Session;
import io.prestosql.cube.CubeManager;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Expression;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.hetu.core.spi.cube.CubeStatus.READY;
import static io.prestosql.cube.CubeManager.STAR_TREE;
import static java.util.Objects.requireNonNull;

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
        private final String cubeName;
        private final Expression newDataPredicate;
        private final boolean overwrite;
        private boolean closed;

        public CubeFinishOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Session session,
                CubeManager cubeManager,
                String cubeName,
                Expression newDataPredicate,
                boolean overwrite)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.session = requireNonNull(session, "session is null");
            this.cubeManager = requireNonNull(cubeManager, "cubeManager is null");
            this.cubeName = requireNonNull(cubeName, "starTableName is null");
            this.newDataPredicate = newDataPredicate;
            this.overwrite = overwrite;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, CubeFinishOperator.class.getSimpleName());
            return new CubeFinishOperator(context, cubeManager, cubeName, newDataPredicate, overwrite);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new CubeFinishOperatorFactory(operatorId, planNodeId, session, cubeManager, cubeName, newDataPredicate, overwrite);
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
    private final String cubeName;
    private final Expression newDataPredicate;
    private final boolean overwrite;
    private State state = State.NEEDS_INPUT;
    private Page page;

    public CubeFinishOperator(
            OperatorContext operatorContext,
            CubeManager cubeManager,
            String cubeName,
            Expression newDataPredicate,
            boolean overwrite)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.cubeMetastore = cubeManager.getMetaStore(STAR_TREE).get();
        this.cubeName = cubeName;
        this.newDataPredicate = newDataPredicate;
        this.overwrite = overwrite;
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
        CubeMetadata cubeMetadata = cubeMetastore.getMetadataFromCubeName(cubeName).get();
        CubeMetadataBuilder builder = cubeMetastore.getBuilder(cubeMetadata);
        Expression updatable;
        if (overwrite || cubeMetadata.getPredicateString() == null) {
            updatable = newDataPredicate;
        }
        else {
            Expression existing = new SqlParser().createExpression(cubeMetadata.getPredicateString(), new ParsingOptions());
            updatable = ExpressionUtils.or(existing, newDataPredicate);
        }
        //TODO: Add Logic to simplify expression. Check if Two between predicates can be merged into one
        builder.withPredicate(ExpressionFormatter.formatExpression(updatable, Optional.empty()));
        builder.setCubeStatus(READY);
        CubeMetadata update = builder.build(System.currentTimeMillis());
        cubeMetastore.persist(update);
        state = State.FINISHED;
        return page;
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
