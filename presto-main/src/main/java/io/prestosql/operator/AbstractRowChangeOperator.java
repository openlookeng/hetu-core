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
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.requireNonNull;

// Table delete doesn't need snapshot support
@RestorableConfig(unsupported = true)
public abstract class AbstractRowChangeOperator
        implements Operator
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARBINARY);

    protected enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;

    protected State state = State.RUNNING;
    protected long rowCount;
    private boolean closed;
    private ListenableFuture<Collection<Slice>> finishFuture;
    private Supplier<Optional<UpdatablePageSource>> pageSource = Optional::empty;

    public AbstractRowChangeOperator(OperatorContext operatorContext)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
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
            finishFuture = toListenableFuture(pageSource().finish());
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.RUNNING;
    }

    @Override
    public abstract void addInput(Page page);

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (finishFuture == null) {
            return NOT_BLOCKED;
        }
        return finishFuture;
    }

    @Override
    public Page getOutput()
    {
        if ((state != State.FINISHING) || !finishFuture.isDone()) {
            return null;
        }
        state = State.FINISHED;

        Collection<Slice> fragments = getFutureValue(finishFuture);

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder page = new PageBuilder(fragments.size() + 1, TYPES);
        BlockBuilder rowsBuilder = page.getBlockBuilder(0);
        BlockBuilder fragmentBuilder = page.getBlockBuilder(1);

        // write row count
        page.declarePosition();
        BIGINT.writeLong(rowsBuilder, rowCount);
        fragmentBuilder.appendNull();

        // write fragments
        for (Slice fragment : fragments) {
            page.declarePosition();
            rowsBuilder.appendNull();
            VARBINARY.writeSlice(fragmentBuilder, fragment);
        }

        return page.build();
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            if (finishFuture != null) {
                finishFuture.cancel(true);
            }
            else {
                pageSource.get().ifPresent(UpdatablePageSource::abort);
            }
        }
    }

    public void setPageSource(Supplier<Optional<UpdatablePageSource>> pageSource)
    {
        this.pageSource = requireNonNull(pageSource, "pageSource is null");
    }

    protected UpdatablePageSource pageSource()
    {
        Optional<UpdatablePageSource> source = pageSource.get();
        checkState(source.isPresent(), "UpdatablePageSource not set");
        return source.get();
    }
}
