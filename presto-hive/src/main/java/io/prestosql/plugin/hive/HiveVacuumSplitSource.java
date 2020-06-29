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
     * buckets->partition->(type of delta)->List of HiveSplits
     */
    private Map<Integer, Map<String, Map<Boolean, List<HiveSplit>>>> splitsMap = new HashMap<>();
    private HiveVacuumTableHandle vacuumTableHandle;
    private HdfsEnvironment hdfsEnvironment;
    private HdfsContext hdfsContext;

    HiveVacuumSplitSource(HiveSplitSource splitSource, HiveVacuumTableHandle vacuumTableHandle, HdfsEnvironment hdfsEnvironment, HdfsContext hdfsContext)
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
        Map<String, Map<Boolean, List<HiveSplit>>> bucketMap = splitsMap.get(bucketNumber);
        if (bucketMap == null) {
            bucketMap = new HashMap<>();
            splitsMap.put(bucketNumber, bucketMap);
        }
        Map<Boolean, List<HiveSplit>> partitionMap = getPartitionMap(partition, bucketMap);
        return getSplitsFromPartition(isDeleteDelta, partitionMap);
    }

    private Map<Boolean, List<HiveSplit>> getPartitionMap(String partition, Map<String, Map<Boolean, List<HiveSplit>>> bucketMap)
    {
        if (partition == null) {
            partition = "default";
        }
        Map<Boolean, List<HiveSplit>> partitionMap = bucketMap.get(partition);
        if (partitionMap == null) {
            partitionMap = new HashMap<>();
            bucketMap.put(partition, partitionMap);
        }
        return partitionMap;
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
                    int bucketNumber = getBucketNumber(hiveSplit);
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
         * All splits are grouped separately based on the bucketNumber and type of the file if minor compaction is enabled.
         *  if partitionHandle is available, then noMoreSplits=true indicates only for specific bucket.
         *  else, noMoreSplits=true indicates all splits are over.
         */
        List<HiveSplit> bucketedSplits = null;
        int bucketNumber = 0;
        boolean noMoreSplits = false;

        /*
         * If partition handle is passed splits will be chosen only for that bucket, else will be choosen from overall.
         */
        int bucketToChoose = (partitionHandle instanceof HivePartitionHandle) ? ((HivePartitionHandle) partitionHandle).getBucket() : -1;
        while (true) {
            Map<String, Map<Boolean, List<HiveSplit>>> bucketEntry = null;
            if (bucketToChoose != -1) {
                bucketEntry = splitsMap.get(bucketToChoose);
                bucketNumber = bucketToChoose;
            }
            else {
                Iterator<Entry<Integer, Map<String, Map<Boolean, List<HiveSplit>>>>> it = splitsMap.entrySet().iterator();
                if (it.hasNext()) {
                    Entry<Integer, Map<String, Map<Boolean, List<HiveSplit>>>> entry = it.next();
                    bucketEntry = entry.getValue();
                    bucketNumber = entry.getKey();
                }
            }
            if (bucketEntry == null) {
                //No more entries
                noMoreSplits = true;
                break;
            }
            Iterator<Map<Boolean, List<HiveSplit>>> partitionMapIterator = bucketEntry.values().iterator();
            while (partitionMapIterator.hasNext()) {
                Map<Boolean, List<HiveSplit>> deltaTypeMap = partitionMapIterator.next();
                Iterator<List<HiveSplit>> splitsIterator = deltaTypeMap.values().iterator();
                if (splitsIterator.hasNext()) {
                    //Choose the splits and remove the entry
                    bucketedSplits = splitsIterator.next();
                    splitsIterator.remove();
                }
                if (!splitsIterator.hasNext()) {
                    partitionMapIterator.remove();
                }
                if (bucketedSplits != null) {
                    break;
                }
            }
            if (bucketEntry.size() == 0) {
                //No more bucket entries available.
                splitsMap.remove(bucketNumber);
            }

            if (bucketedSplits != null) {
                //check whether splits are selected for already compacted files.
                if (bucketedSplits.size() == 1) {
                    HiveSplit split = bucketedSplits.get(0);

                    Path bucketFile = new Path(split.getPath());
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
                    Range suitableRange = getOnlyElement(vacuumTableHandle.getSuitableRange(range));
                    if (range.equals(suitableRange)) {
                        //Already compacted
                        // or only one participant, which makes no sense to compact
                        //Check for some more.
                        bucketedSplits = null;
                        continue;
                    }
                }
                //Decide the noMoreSplits based on the request type.
                //If the request was for specific bucket, then it noMoreSplits applies only for bucket, otherwise overall.
                noMoreSplits = (bucketToChoose != -1) ? bucketEntry.size() == 0 : splitsMap.size() == 0;
                break;
            }
        }
        if (bucketedSplits != null && !bucketedSplits.isEmpty()) {
            HiveSplitWrapper multiSplit = new HiveSplitWrapper(bucketedSplits, OptionalInt.of(bucketNumber));
            //All of the splits for this bucket number are grouped, There are no more splits for this bucket.
            return new ConnectorSplitBatch(ImmutableList.of(multiSplit), noMoreSplits);
        }
        else {
            return new ConnectorSplitBatch(ImmutableList.of(), true);
        }
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
