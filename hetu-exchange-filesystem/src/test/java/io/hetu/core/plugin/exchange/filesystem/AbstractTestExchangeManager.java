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
package io.hetu.core.plugin.exchange.filesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.exchange.Exchange;
import io.prestosql.spi.exchange.ExchangeContext;
import io.prestosql.spi.exchange.ExchangeManager;
import io.prestosql.spi.exchange.ExchangeSink;
import io.prestosql.spi.exchange.ExchangeSinkHandle;
import io.prestosql.spi.exchange.ExchangeSinkInstanceHandle;
import io.prestosql.spi.exchange.ExchangeSource;
import io.prestosql.spi.exchange.ExchangeSourceHandle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.hetu.core.plugin.exchange.filesystem.FileSystemExchangeErrorCode.MAX_OUTPUT_PARTITION_COUNT_EXCEEDED;
import static io.prestosql.spi.exchange.ExchangeId.createRandomExchangeId;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractTestExchangeManager
{
    private ExchangeManager exchangeManager;

    @BeforeClass
    public void init()
    {
        exchangeManager = createExchangeManager();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        if (exchangeManager != null) {
            exchangeManager = null;
        }
    }

    protected abstract ExchangeManager createExchangeManager();

    @Test
    public void testHappyPath() throws ExecutionException, InterruptedException
    {
        Exchange exchange = exchangeManager.createExchange(new ExchangeContext(new QueryId("query"), createRandomExchangeId()), 2);
        ExchangeSinkHandle sinkHandle0 = exchange.addSink(0);
        ExchangeSinkHandle sinkHandle1 = exchange.addSink(1);
        ExchangeSinkHandle sinkHandle2 = exchange.addSink(2);
        exchange.noMoreSinks();

        ExchangeSinkInstanceHandle sinkInstanceHandle = exchange.instantiateSink(sinkHandle0, 0);
        writeData(
                sinkInstanceHandle,
                ImmutableMultimap.of(
                        0, "0-0-0",
                        1, "0-1-0",
                        0, "0-0-1",
                        1, "0-1-1"),
                true);
        exchange.sinkFinished(sinkInstanceHandle);
        sinkInstanceHandle = exchange.instantiateSink(sinkHandle0, 1);
        writeData(
                sinkInstanceHandle,
                ImmutableMultimap.of(
                        0, "0-0-0",
                        1, "0-1-0",
                        0, "0-0-1",
                        1, "0-1-1"),
                true);
        exchange.sinkFinished(sinkInstanceHandle);
        sinkInstanceHandle = exchange.instantiateSink(sinkHandle0, 2);
        writeData(
                sinkInstanceHandle,
                ImmutableMultimap.of(
                        0, "failed",
                        1, "another failed"),
                false);
        exchange.sinkFinished(sinkInstanceHandle);

        sinkInstanceHandle = exchange.instantiateSink(sinkHandle1, 0);
        writeData(
                sinkInstanceHandle,
                ImmutableMultimap.of(
                        0, "1-0-0",
                        1, "1-1-0",
                        0, "1-0-1",
                        1, "1-1-1"),
                true);
        exchange.sinkFinished(sinkInstanceHandle);
        sinkInstanceHandle = exchange.instantiateSink(sinkHandle1, 1);
        writeData(
                sinkInstanceHandle,
                ImmutableMultimap.of(
                        0, "1-0-0",
                        1, "1-1-0",
                        0, "1-0-1",
                        1, "1-1-1"),
                true);
        exchange.sinkFinished(sinkInstanceHandle);
        sinkInstanceHandle = exchange.instantiateSink(sinkHandle1, 2);
        writeData(
                sinkInstanceHandle,
                ImmutableMultimap.of(
                        0, "failed",
                        1, "another failed"),
                false);
        exchange.sinkFinished(sinkInstanceHandle);

        sinkInstanceHandle = exchange.instantiateSink(sinkHandle2, 2);
        writeData(
                sinkInstanceHandle,
                ImmutableMultimap.of(
                        0, "2-0-0",
                        1, "2-1-0"),
                true);
        exchange.sinkFinished(sinkInstanceHandle);

        List<ExchangeSourceHandle> partitionHandles = exchange.getSourceHandles().get();
        assertThat(partitionHandles).hasSize(2);

        Map<Integer, ExchangeSourceHandle> partitions = partitionHandles.stream()
                .collect(toImmutableMap(ExchangeSourceHandle::getPartitionId, Function.identity()));
        assertThat(readData(partitions.get(0)))
                .containsExactlyInAnyOrder("0-0-0", "0-0-1", "1-0-0", "1-0-1", "2-0-0");
        assertThat(readData(partitions.get(1)))
                .containsExactlyInAnyOrder("0-1-0", "0-1-1", "1-1-0", "1-1-1", "2-1-0");

        exchange.close();
    }

    private String repeatString(String s, long r)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < r; i++) {
            builder.append(s);
        }
        return builder.toString();
    }

    @Test
    public void testLargePages() throws ExecutionException, InterruptedException
    {
        String smallPage = repeatString("a", toIntExact(new DataSize(123, BYTE).toBytes()));
        String mediumPage = repeatString("b", toIntExact(new DataSize(66, KILOBYTE).toBytes()));
        String largePage = repeatString("c", toIntExact(new DataSize(5, MEGABYTE).toBytes()));
        String maxPage = repeatString("d", toIntExact(new DataSize(16, MEGABYTE).toBytes()));

        Exchange exchange = exchangeManager.createExchange(new ExchangeContext(new QueryId("query"), createRandomExchangeId()), 3);
        ExchangeSinkHandle sinkHandle0 = exchange.addSink(0);
        ExchangeSinkHandle sinkHandle1 = exchange.addSink(1);
        ExchangeSinkHandle sinkHandle2 = exchange.addSink(2);
        exchange.noMoreSinks();

        ExchangeSinkInstanceHandle sinkInstanceHandle = exchange.instantiateSink(sinkHandle0, 0);
        writeData(
                sinkInstanceHandle,
                new ImmutableMultimap.Builder<Integer, String>()
                        .putAll(0, ImmutableList.of(smallPage))
                        .putAll(1, ImmutableList.of(maxPage, mediumPage))
                        .putAll(2, ImmutableList.of())
                        .build(),
                true);
        exchange.sinkFinished(sinkInstanceHandle);

        sinkInstanceHandle = exchange.instantiateSink(sinkHandle1, 0);
        writeData(
                sinkInstanceHandle,
                new ImmutableMultimap.Builder<Integer, String>()
                        .putAll(0, ImmutableList.of(mediumPage))
                        .putAll(1, ImmutableList.of(largePage))
                        .putAll(2, ImmutableList.of(smallPage))
                        .build(),
                true);
        exchange.sinkFinished(sinkInstanceHandle);

        sinkInstanceHandle = exchange.instantiateSink(sinkHandle2, 0);
        writeData(
                sinkInstanceHandle,
                new ImmutableMultimap.Builder<Integer, String>()
                        .putAll(0, ImmutableList.of(largePage, maxPage))
                        .putAll(1, ImmutableList.of(smallPage))
                        .putAll(2, ImmutableList.of(maxPage, largePage, mediumPage))
                        .build(),
                true);
        exchange.sinkFinished(sinkInstanceHandle);

        List<ExchangeSourceHandle> partitionHandles = exchange.getSourceHandles().get();
        assertThat(partitionHandles).hasSize(3);

        Map<Integer, ExchangeSourceHandle> partitions = partitionHandles.stream()
                .collect(toImmutableMap(ExchangeSourceHandle::getPartitionId, Function.identity()));

        assertThat(readData(partitions.get(0)))
                .containsExactlyInAnyOrder(smallPage, mediumPage, largePage, maxPage);
    }

    @Test
    public void testMaxOutputPartitionCountCheck()
    {
        assertThatThrownBy(() -> exchangeManager.createExchange(new ExchangeContext(new QueryId("query"), createRandomExchangeId()), 51))
                .hasMessageContaining("Max number of output partitions exceeded for exchange")
                .hasFieldOrPropertyWithValue("errorCode", MAX_OUTPUT_PARTITION_COUNT_EXCEEDED.toErrorCode());
    }

    private void writeData(ExchangeSinkInstanceHandle handle, Multimap<Integer, String> data, boolean finish)
    {
        ExchangeSink sink = exchangeManager.createSink(handle, false);
        data.forEach((key, value) -> {
            Slice slice = Slices.utf8Slice(value);
            SliceOutput sliceOutput = Slices.allocate(slice.length() + Integer.BYTES).getOutput();
            sliceOutput.writeInt(slice.length());
            sliceOutput.writeBytes(slice);
            sink.add(key, sliceOutput.slice());
        });
        if (finish) {
            getFutureValue(sink.finish());
        }
        else {
            getFutureValue(sink.abort());
        }
    }

    private List<String> readData(ExchangeSourceHandle handle)
    {
        return readData(ImmutableList.of(handle));
    }

    private List<String> readData(List<ExchangeSourceHandle> handles)
    {
        ImmutableList.Builder<String> result = ImmutableList.builder();
        try (ExchangeSource source = exchangeManager.createSource(handles)) {
            while (!source.isFinished()) {
                Slice data = source.read();
                if (data != null) {
                    String s = data.toStringUtf8();
                    result.add(s);
                }
            }
        }
        return result.build();
    }
}
