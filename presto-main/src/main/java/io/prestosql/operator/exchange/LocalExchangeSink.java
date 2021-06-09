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
package io.prestosql.operator.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.MarkerPage;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.operator.Operator.NOT_BLOCKED;
import static io.prestosql.operator.exchange.LocalExchanger.FINISHED;
import static java.util.Objects.requireNonNull;

public class LocalExchangeSink
{
    public static LocalExchangeSink finishedLocalExchangeSink(boolean isForMerge)
    {
        LocalExchangeSink finishedSink = new LocalExchangeSink(null, FINISHED, sink -> {}, isForMerge);
        finishedSink.finish();
        return finishedSink;
    }

    // Snapshot: Local-exchange is used to broadcast markers, because it has all the local-sources
    private final LocalExchange exchange;
    private final LocalExchanger exchanger;
    private final Consumer<LocalExchangeSink> onFinish;

    private final AtomicBoolean finished = new AtomicBoolean();

    // Snapshot: broadcast markers for local-exchange operator, and pass-through for local-merge operator
    private final boolean isForMerge;

    public LocalExchangeSink(
            LocalExchange exchange,
            LocalExchanger exchanger,
            Consumer<LocalExchangeSink> onFinish,
            boolean isForMerge)
    {
        this.exchange = exchange;
        this.exchanger = requireNonNull(exchanger, "exchanger is null");
        this.onFinish = requireNonNull(onFinish, "onFinish is null");
        this.isForMerge = isForMerge;
    }

    public void finish()
    {
        if (finished.compareAndSet(false, true)) {
            exchanger.finish();
            onFinish.accept(this);
        }
    }

    public boolean isFinished()
    {
        return finished.get();
    }

    public void addPage(Page page, String origin)
    {
        requireNonNull(page, "page is null");

        // ignore pages after finished
        // this can happen with limit queries when all of the source (readers) are closed, so sinks
        // can be aborted early
        if (isFinished()) {
            return;
        }

        // there can be a race where finished is set between the check above and here
        // it is expected that the exchanger ignores pages after finish
        if (!isForMerge && page instanceof MarkerPage) {
            // Bypass exchanger, and ask exchange to broadcast the marker to all targets.
            checkState(exchange != null);
            exchange.broadcastMarker((MarkerPage) page, origin);
            // For merges, exchanger is always pass-through, and all sources are merged by a single operator instance,
            // so there is no need to "broadcast". Use pass-through exchanger to send markers
            // (so sink->source is considered a single pipeline).
        }
        else {
            exchanger.accept(page, origin);
        }
    }

    public ListenableFuture<?> waitForWriting()
    {
        if (isFinished()) {
            return NOT_BLOCKED;
        }
        return exchanger.waitForWriting();
    }
}
