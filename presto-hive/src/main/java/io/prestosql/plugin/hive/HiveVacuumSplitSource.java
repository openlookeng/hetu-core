/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveVacuumTableHandle.Range;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * This class is used in case of Vacuum operation, where all related (same bucket) splits should be scheduled
 * to same worker and together, to start vacuum operation. So class wrapps all such splits together and scheduled as
 * single split.
 */
class HiveVacuumSplitSource
        implements ConnectorSplitSource
{
    private HiveSplitSource splitSource;
    /*
     * Vacuum operations can be made to run in parallel for the maximum extent as below.
     * Minor Vacuums will also have delete_delta splits separately, which can be run in parallel.
     * Each grouped splits will be scheduled separately and run in parallel if enough number of workers available.
     * Splits are grouped as below to execute in parallel.
     * partition->buckets->(type of delta)->List of HiveSplits
     */
    private Map<String, Map<Integer, Map<Boolean, List<HiveSplit>>>> splitsMap = new HashMap<>();
    private HiveVacuumTableHandle vacuumTableHandle;
    private HdfsEnvironment hdfsEnvironment;
    private HdfsContext hdfsContext;

    HiveVacuumSplitSource(HiveSplitSource splitSource, HiveVacuumTableHandle vacuumTableHandle, HdfsEnvironment hdfsEnvironment, HdfsContext hdfsContext, ConnectorSession session)
    {
        this.splitSource = splitSource;
        this.vacuumTableHandle = vacuumTableHandle;
        this.hdfsContext = hdfsContext;
        this.hdfsEnvironment = hdfsEnvironment;
    }

    private int getBucketNumber(HiveSplit hiveSplit)
    {
        if (hiveSplit.getBucketNumber().isPresent()) {
            return hiveSplit.getBucketNumber().getAsInt();
        }

        Path bucketFile = new Path(hiveSplit.getFilePath());
        OptionalInt bucketNumber = HiveUtil.getBucketNumber(bucketFile.getName());
        return bucketNumber.orElse(0);
    }

    private boolean isDeleteDelta(HiveSplit hiveSplit)
    {
        Path bucketFile = new Path(hiveSplit.getPath());
        return AcidUtils.isDeleteDelta(bucketFile.getParent());
    }

    private List<HiveSplit> getHiveSplitsFor(int bucketNumber, String partition, boolean isDeleteDelta)
    {
        if (partition == null) {
            partition = "default";
        }
        Map<Integer, Map<Boolean, List<HiveSplit>>> bucketToSplits = splitsMap.get(partition);
        if (bucketToSplits == null) {
            bucketToSplits = new HashMap<>();
            splitsMap.put(partition, bucketToSplits);
        }
        Map<Boolean, List<HiveSplit>> partitionMap = getDeltaTypeToSplitsMap(bucketNumber, bucketToSplits);
        return getSplitsFromPartition(isDeleteDelta, partitionMap);
    }

    private Map<Boolean, List<HiveSplit>> getDeltaTypeToSplitsMap(int bucketNumber, Map<Integer, Map<Boolean, List<HiveSplit>>> bucketsToSplits)
    {
        Map<Boolean, List<HiveSplit>> deltaTypeToSplits = bucketsToSplits.get(bucketNumber);
        if (deltaTypeToSplits == null) {
            deltaTypeToSplits = new HashMap<>();
            bucketsToSplits.put(bucketNumber, deltaTypeToSplits);
        }
        return deltaTypeToSplits;
    }

    private List<HiveSplit> getSplitsFromPartition(boolean isDeleteDelta, Map<Boolean, List<HiveSplit>> partitionMap)
    {
        List<HiveSplit> hiveSplits = partitionMap.get(isDeleteDelta);
        if (hiveSplits == null) {
            hiveSplits = new ArrayList<>();
            partitionMap.put(isDeleteDelta, hiveSplits);
        }
        return hiveSplits;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        do {
            CompletableFuture<ConnectorSplitBatch> nextBatch = splitSource.getNextBatch(partitionHandle, maxSize);
            try {
                ConnectorSplitBatch splitBatch = nextBatch.get();
                List<ConnectorSplit> splits = splitBatch.getSplits();
                for (ConnectorSplit split : splits) {
                    HiveSplit hiveSplit = ((HiveSplitWrapper) split).getSplits().get(0);
                    int bucketNumber = vacuumTableHandle.isUnifyVacuum() ? 0 : getBucketNumber(hiveSplit); //In case of unify there are no bucket numbers
                    boolean isDeleteDelta = isDeleteDelta(hiveSplit);
                    List<HiveSplit> hiveSplits = getHiveSplitsFor(bucketNumber, hiveSplit.getPartitionName(), isDeleteDelta);
                    hiveSplits.add(hiveSplit);
                }
                if (splitBatch.isNoMoreSplits()) {
                    break;
                }
            }
            catch (InterruptedException e) {
                HiveSplitSource.propagatePrestoException(e);
            }
            catch (ExecutionException e) {
                HiveSplitSource.propagatePrestoException(e.getCause());
            }
        }
        while (true);

        ConnectorSplitBatch splitBatch = getCurrentBatch(partitionHandle);
        return completedFuture(splitBatch);
    }

    private ConnectorSplitBatch getCurrentBatch(ConnectorPartitionHandle partitionHandle)
    {
        /*
         * All Splits are grouped based on partition->bucketNumber->(delta_type in case of Minor vacuum).
         */
        List<HiveSplit> bucketedSplits = null;
        int bucketNumber = 0;

        /*
         * If partition handle is passed splits will be chosen only for that bucket, else will be choosen from available buckets.
         */
        int bucketToChoose = (partitionHandle instanceof HivePartitionHandle) ? ((HivePartitionHandle) partitionHandle).getBucket() : -1;
        Iterator<Entry<String, Map<Integer, Map<Boolean, List<HiveSplit>>>>> partitions = splitsMap.entrySet().iterator();
        while (partitions.hasNext()) {
            Entry<String, Map<Integer, Map<Boolean, List<HiveSplit>>>> currentPartitionEntry = partitions.next();
            String currentPartition = currentPartitionEntry.getKey();
            if (vacuumTableHandle.isUnifyVacuum() && currentPartition.contains(HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION)) {
                //skip the dynamic partition for now.
                partitions.remove();
                continue;
            }
            Map<Integer, Map<Boolean, List<HiveSplit>>> buckets = currentPartitionEntry.getValue();
            Map<Boolean, List<HiveSplit>> deltaTypeToSplits = null;
            if (bucketToChoose != -1) {
                deltaTypeToSplits = buckets.get(bucketToChoose);
                bucketNumber = bucketToChoose;
            }
            else {
                Iterator<Entry<Integer, Map<Boolean, List<HiveSplit>>>> deltaTypeIterator = buckets.entrySet().iterator();
                if (deltaTypeIterator.hasNext()) {
                    Entry<Integer, Map<Boolean, List<HiveSplit>>> entry = deltaTypeIterator.next();
                    deltaTypeToSplits = entry.getValue();
                    bucketNumber = entry.getKey();
                }
            }
            if (deltaTypeToSplits == null) {
                if (buckets.size() == 0) {
                    partitions.remove();
                }
                continue;
            }
            Iterator<Entry<Boolean, List<HiveSplit>>> splitsIterator = deltaTypeToSplits.entrySet().iterator();
            boolean type;
            if (splitsIterator.hasNext()) {
                //Choose the splits and remove the entry
                Entry<Boolean, List<HiveSplit>> entry = splitsIterator.next();
                bucketedSplits = entry.getValue();
                splitsIterator.remove();
            }
            if (!splitsIterator.hasNext()) {
                buckets.remove(bucketNumber);
                if (buckets.size() == 0) {
                    partitions.remove();
                }
            }
            if (bucketedSplits == null || bucketedSplits.isEmpty()) {
                continue;
            }

            if (bucketedSplits != null) {
                //check whether splits are selected for already compacted files.
                if (bucketedSplits.size() == 1) {
                    HiveSplit split = bucketedSplits.get(0);

                    Path bucketFile = new Path(split.getPath());
                    Range range = getRange(bucketFile);
                    Range suitableRange = getOnlyElement(vacuumTableHandle.getSuitableRange(currentPartition, range));
                    if (range.equals(suitableRange)) {
                        //Already compacted
                        // or only one participant, which makes no sense to compact
                        //Check for some more.
                        bucketedSplits = null;
                        continue;
                    }
                }
                break;
            }
        }
        if (bucketedSplits != null && !bucketedSplits.isEmpty()) {
            HiveSplitWrapper multiSplit = new HiveSplitWrapper(bucketedSplits, OptionalInt.of(bucketNumber));
            //All of the splits for this bucket inside one partition are grouped.
            //There may be some more splits for same bucket in different partition.
            return new ConnectorSplitBatch(ImmutableList.of(multiSplit), false);
        }
        else {
            return new ConnectorSplitBatch(ImmutableList.of(), true);
        }
    }

    private Range getRange(Path bucketFile)
    {
        Range range;
        try {
            Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, bucketFile);
            Options options = hdfsEnvironment.doAs(hdfsContext.getIdentity().getUser(), () ->
                    AcidUtils.parseBaseOrDeltaBucketFilename(bucketFile, configuration));
            range = new Range(options.getMinimumWriteId(), options.getMaximumWriteId());
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, "Error while parsing split info for vacuum", e);
        }
        return range;
    }

    @Override
    public void close()
    {
        splitSource.close();
    }

    @Override
    public boolean isFinished()
    {
        return splitSource.isFinished();
    }
}
