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
package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.memory.data.LogicalPart;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

public final class MemorySplitManager
        implements ConnectorSplitManager
{
    private final MemoryMetadata metadata;

    @Inject
    public MemorySplitManager(MemoryConfig config, MemoryMetadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableHandle handle, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        MemoryTableHandle table = (MemoryTableHandle) handle;

        List<MemoryDataFragment> dataFragments = metadata.getDataFragments(table.getId());

        // check if there is a predicate on the partition column
        List<SortedRangeSet> partitionKeyRanges = new ArrayList<>();
        for (Map.Entry<ColumnHandle, Domain> e : table.getPredicate().getDomains().orElse(Collections.emptyMap()).entrySet()) {
            if (!e.getKey().isPartitionKey()) {
                continue;
            }

            // in LogicalPart#partitionPage null partition is a special case
            // although the partition map in LogicalPart class can have null keys
            // when the map is sent to coordinator via MemoryDataFragment the null
            // keys are skipped because the JSON parser can't handle null keys in a map
            // therefore when query predicate contains a null value, we MUST NOT
            // do any partition filtering because we would miss data
            //
            // e.g. if query is: select * from table where column is null
            // then schedule all the splits
            //
            // see comment in LogicalPart#partitionPage also
            if (e.getValue().isNullAllowed()) {
                return allSplits(dataFragments, table);
            }
            SortedRangeSet rangeSet = ((SortedRangeSet) e.getValue().getValues());
            partitionKeyRanges.add(rangeSet);
        }

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        if (partitionKeyRanges.isEmpty()) {
            return allSplits(dataFragments, table);
        }

        for (MemoryDataFragment dataFragment : dataFragments) {
            Map<String, List<Integer>> logicalPartPartitionMap = dataFragment.getLogicalPartPartitionMap();
            long rows = dataFragment.getRows();

            if (logicalPartPartitionMap.size() == 0) {
                int logicalPartCount = dataFragment.getLogicalPartCount();
                // logicalPart ids are 1 based
                for (int i = 1; i <= logicalPartCount; i++) {
                    splits.add(new MemorySplit(table.getId(), i, dataFragment.getHostAddress(), rows, OptionalLong.empty()));
                }
            }
            else {
                // filter the splits based on the partitionKey and only schedule them
                for (Map.Entry<String, List<Integer>> entry : logicalPartPartitionMap.entrySet()) {
                    for (SortedRangeSet rangeSet : partitionKeyRanges) {
                        Type rangeSetType = rangeSet.getType();
                        Object value = LogicalPart.deserializeTypedValueFromString(rangeSetType, entry.getKey());
                        if (rangeSet.containsValue(value)) {
                            for (Integer i : entry.getValue()) {
                                splits.add(new MemorySplit(table.getId(), i, dataFragment.getHostAddress(), rows, OptionalLong.empty()));
                            }
                        }
                    }
                }
            }
        }
        return new FixedSplitSource(splits.build());
    }

    /**
     * Schedule entire table
     */
    private ConnectorSplitSource allSplits(List<MemoryDataFragment> dataFragments, MemoryTableHandle table)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (MemoryDataFragment dataFragment : dataFragments) {
            int logicalPartCount = dataFragment.getLogicalPartCount();
            long rows = dataFragment.getRows();
            // logicalPart ids are 1 based
            for (int i = 1; i <= logicalPartCount; i++) {
                splits.add(new MemorySplit(table.getId(), i, dataFragment.getHostAddress(), rows, OptionalLong.empty()));
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
