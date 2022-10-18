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
package io.prestosql.spi.connector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.StreamSupport.stream;

public class FixedSplitSource
        implements ConnectorSplitSource
{
    private final List<ConnectorSplit> splits;
    private int offset;

    private final Optional<List<Object>> tableExecuteSplitsInfo;

    public FixedSplitSource(Iterable<? extends ConnectorSplit> splits)
    {
        requireNonNull(splits, "splits is null");
        List<ConnectorSplit> splitsList = new ArrayList<>();
        for (ConnectorSplit split : splits) {
            splitsList.add(split);
        }
        this.splits = Collections.unmodifiableList(splitsList);
        this.tableExecuteSplitsInfo = null;
    }

    public FixedSplitSource(Iterable<? extends ConnectorSplit> splits, List<Object> tableExecuteSplitsInfo)
    {
        this(splits, Optional.of(tableExecuteSplitsInfo));
    }

    private FixedSplitSource(Iterable<? extends ConnectorSplit> splits, Optional<List<Object>> tableExecuteSplitsInfo)
    {
        requireNonNull(splits, "splits is null");
        requireNonNull(tableExecuteSplitsInfo, "tableExecuteSplitsInfo is null");
        this.splits = stream(splits.spliterator(), false).collect(Collectors.collectingAndThen(Collectors.toList(), x -> Collections.unmodifiableList(x)));
        this.tableExecuteSplitsInfo = requireNonNull(tableExecuteSplitsInfo, "tableExecuteSplitsInfo is null").map(item -> Collections.unmodifiableList(item));
    }

    @SuppressWarnings("ObjectEquality")
    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        if (!partitionHandle.equals(NOT_PARTITIONED)) {
            throw new IllegalArgumentException("partitionHandle must be NOT_PARTITIONED");
        }

        int remainingSplits = splits.size() - offset;
        int size = Math.min(remainingSplits, maxSize);
        List<ConnectorSplit> results = splits.subList(offset, offset + size);
        offset += size;

        return completedFuture(new ConnectorSplitBatch(results, isFinished()));
    }

    @Override
    public boolean isFinished()
    {
        return offset >= splits.size();
    }

    @Override
    public void close()
    {
    }
}
