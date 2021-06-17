/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.operator.HashGenerator;
import io.prestosql.operator.InterpretedHashGenerator;
import io.prestosql.operator.PrecomputedHashGenerator;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.SystemSessionProperties.getPrcntDriversForPartialAggr;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class SortBasedAggregationPartitioningExchanger
        implements LocalExchanger
{
    private static final int finalizedChannel = 2;

    protected final List<BiConsumer<PageReference, String>> buffers;
    protected final LocalExchangeMemoryManager memoryManager;
    private LocalPartitionGenerator partitionGenerator;

    private long queryIdHashCode;
    private int percentDriversForUnFinalizedValues;
    private final IntArrayList[] partitionAssignments;
    private int hashPartitionIndexStartPosition;
    private int numberOfHashPartitionLocations;
    private int numberOfSortBasedPartitionLocations;
    protected int[] hashPartitionLocationsMapping;
    protected int[] sortBasedPartitionLocationsMapping;

    //1) In case of SortBased aggregation un-finalized values are assigned to one set of drivers (this drivers uses Hash Aggr).
    //2) not finalized values should got to same driver (ex: Page 1 contains value 12 , driver 3 processed it, page 2 contains value 12, so it should pass to same driver.
    //3) finalized vales are assigned different set of drivers(this drivers uses sort Based Aggr).
    public SortBasedAggregationPartitioningExchanger(List<BiConsumer<PageReference, String>> partitions,
            LocalExchangeMemoryManager memoryManager,
            List<? extends Type> types,
            List<Integer> partitionChannels,
            Optional<Integer> hashChannel,
            Session session)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.queryIdHashCode = Integer.toUnsignedLong(session.getQueryId().hashCode());
        this.percentDriversForUnFinalizedValues = getPrcntDriversForPartialAggr(session);

        partitionAssignments = new IntArrayList[partitions.size()];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
        }

        int numberOfPartitions = partitionAssignments.length;
        this.hashPartitionIndexStartPosition = (int) (queryIdHashCode % numberOfPartitions);
        this.numberOfHashPartitionLocations = (numberOfPartitions * percentDriversForUnFinalizedValues) / 100;
        if (numberOfHashPartitionLocations == 0) {
            numberOfHashPartitionLocations = 1;
        }
        else {
            numberOfHashPartitionLocations = roundOffToPowerOfTwo(numberOfHashPartitionLocations);
        }
        this.numberOfSortBasedPartitionLocations = numberOfPartitions - numberOfHashPartitionLocations;
        hashPartitionLocationsMapping = new int[numberOfHashPartitionLocations];
        sortBasedPartitionLocationsMapping = new int[numberOfSortBasedPartitionLocations];

        if (numberOfHashPartitionLocations > 1) {
            hashPartitionIndexStartPosition = numberOfPartitions - numberOfHashPartitionLocations;
        }

        int sortArrayIndex = 0;
        int hashArrayIndex = 0;
        if (numberOfHashPartitionLocations == 1) {
            for (int i = 0; i < numberOfPartitions; i++) {
                if (hashPartitionIndexStartPosition != i) {
                    // stores location of partial partition
                    sortBasedPartitionLocationsMapping[sortArrayIndex] = i;
                    sortArrayIndex++;
                }
            }
        }
        else {
            for (int i = 0; i < numberOfPartitions; i++) {
                if (hashPartitionIndexStartPosition <= i) {
                    // stores location of hash partition
                    hashPartitionLocationsMapping[hashArrayIndex] = i;
                    hashArrayIndex++;
                }
                else {
                    sortBasedPartitionLocationsMapping[sortArrayIndex] = i;
                    sortArrayIndex++;
                }
            }
        }

        if (numberOfHashPartitionLocations > 1) {
            HashGenerator hashGenerator;
            if (hashChannel.isPresent()) {
                hashGenerator = new PrecomputedHashGenerator(hashChannel.get());
            }
            else {
                List<Type> partitionChannelTypes = partitionChannels.stream()
                        .map(types::get)
                        .collect(toImmutableList());
                hashGenerator = new InterpretedHashGenerator(partitionChannelTypes, Ints.toArray(partitionChannels));
            }
            this.partitionGenerator = new LocalPartitionGenerator(hashGenerator, numberOfHashPartitionLocations);
        }
    }

    @Override
    public synchronized void accept(Page page, String instanceId)
    {
        // reset the assignment lists
        for (IntList partitionAssignment : partitionAssignments) {
            partitionAssignment.clear();
        }

        int partition;
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (!BOOLEAN.getBoolean(page.getBlock(page.getChannelCount() - finalizedChannel), position)) {
                if (numberOfHashPartitionLocations > 1) {
                    partition = partitionGenerator.getPartition(page, position);
                    partition = hashPartitionLocationsMapping[partition];
                    partitionAssignments[partition].add(position);
                }
                else {
                    partitionAssignments[hashPartitionIndexStartPosition].add(position);
                }
                continue;
            }
            partition = sortBasedPartitionLocationsMapping[position % numberOfSortBasedPartitionLocations];
            partitionAssignments[partition].add(position);
        }

        // build a page for each partition
        Block[] outputBlocks = new Block[page.getChannelCount()];
        for (partition = 0; partition < buffers.size(); partition++) {
            IntArrayList positions = partitionAssignments[partition];
            if (!positions.isEmpty()) {
                for (int i = 0; i < page.getChannelCount(); i++) {
                    outputBlocks[i] = page.getBlock(i).copyPositions(positions.elements(), 0, positions.size());
                }

                Page pageSplit = new Page(positions.size(), outputBlocks);
                memoryManager.updateMemoryUsage(pageSplit.getRetainedSizeInBytes());
                buffers.get(partition).accept(new PageReference(pageSplit, 1, () -> memoryManager.updateMemoryUsage(-pageSplit.getRetainedSizeInBytes())), instanceId);
            }
        }
    }

    @Override
    public ListenableFuture<?> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }

    int roundOffToPowerOfTwo(int n)
    {
        if ((n & (n - 1)) == 0) {
            return n;
        }
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n = n + 1;
        return (n >> 1);
    }
}
