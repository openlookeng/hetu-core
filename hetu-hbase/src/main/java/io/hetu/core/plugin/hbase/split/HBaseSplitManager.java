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
package io.hetu.core.plugin.hbase.split;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.hetu.core.plugin.hbase.utils.Constants;
import io.hetu.core.plugin.hbase.utils.HBaseErrorCode;
import io.hetu.core.plugin.hbase.utils.StartAndEndKey;
import io.hetu.core.plugin.hbase.utils.Utils;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * HBaseSplitManager
 *
 * @since 2020-03-18
 */
public class HBaseSplitManager
        implements ConnectorSplitManager
{
    private static final Logger LOG = Logger.get(HBaseSplitManager.class);

    private final HBaseConnection hbaseConnection;

    @Inject
    public HBaseSplitManager(HBaseConnection hbaseConnection)
    {
        this.hbaseConnection = hbaseConnection;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        HBaseTableHandle tableHandle = (HBaseTableHandle) connectorTableHandle;
        TupleDomain<ColumnHandle> tupleDomain = tableHandle.getConstraint();
        List<HBaseSplit> splits;

        if (Utils.isBatchGet(tupleDomain, tableHandle.getRowIdOrdinal())) {
            splits = getSplitsForBatchGet(tupleDomain, tableHandle);
            Collections.shuffle(splits);
            return new FixedSplitSource(splits);
        }
        else if (hbaseConnection.getHbaseConfig().isClientSideEnable()) {
            splits = getSplitsForClientSide(tupleDomain, tableHandle);
        }
        else {
            splits = getSplitsForScan(tupleDomain, tableHandle);
        }

        return new FixedSplitSource(splits);
    }

    /**
     * Get splits by slicing the rowKeys, according to the first character of rowKey (user can specify it when create
     * table, the default value is "0~9,a~z,A~Z", if your rowKey is chinese, you should set split_by_char = '一~锯'),
     * generate many startAndEndKey pairs.
     *
     * @param tupleDomain tupleDomain
     * @param tableHandle tableHandle
     * @return splits
     */
    private List<HBaseSplit> getSplitsForScan(TupleDomain<ColumnHandle> tupleDomain, HBaseTableHandle tableHandle)
    {
        List<HBaseSplit> splits = new ArrayList<>();
        TableName hbaseTableName = TableName.valueOf(tableHandle.getHbaseTableName().get());
        Map<Integer, List<Range>> ranges = predicateTransferToRanges(tupleDomain.getDomains().get());
        List<HostAddress> hostAddresses = new ArrayList<>();
        // splitByChar read from hetuMetastore, the default value is "0~9,a~z,A~Z"
        String splitByChar = hbaseConnection.getTable(tableHandle.getTableName()).getSplitByChar().get();
        LOG.debug("Create multi-splits by the first char of rowKey, table is " + hbaseTableName.getName() +
                ", the range of first char is : " + splitByChar);

        List<StartAndEndKey> startAndEndRowKeys =
                getStartAndEndKeys(splitByChar, Constants.START_END_KEYS_COUNT);
        for (StartAndEndKey startAndEndRowKey : startAndEndRowKeys) {
            splits.add(
                    new HBaseSplit(
                            tableHandle.getRowId(),
                            tableHandle,
                            hostAddresses,
                            String.valueOf(startAndEndRowKey.getStart()),
                            startAndEndRowKey.getEnd() + Constants.ROWKEY_TAIL,
                            ranges,
                            -1,
                            false,
                            null));
        }

        printSplits("Scan", splits);
        return splits;
    }

    /**
     * In order to get more splits to improve concurrency of tableScan, we slice the split by different character.
     * HBase server support to use startRow and EndRow to get scanner.
     * for example, splitByChar is 0~2, we will generate multi-pairs
     * the size of pairs will less than startEndKeysCount, so we will calculate the pair gap first.
     * (startKey = 0, endKey = 0),(startKey = 1, endKey = 1),(startKey = 2, endKey = 2)
     * splitByChar is 0~9, a~z
     * (startKey = 0, endKey = 1),(startKey = 2, endKey = 3)……(startKey = y, endKey = z)
     *
     * @param splitByChar range of the rowKey, value is like 0~9,A~Z,a~z or a~z,0~9 ..
     * @param startEndKeysCount max number of key pairs
     * @return start and end rowKeys
     */
    private List<StartAndEndKey> getStartAndEndKeys(String splitByChar, int startEndKeysCount)
    {
        List<StartAndEndKey> allRanges = Arrays.stream(splitByChar.split(","))
                .map(StartAndEndKey::new).collect(Collectors.toList());
        int rangeLength = 0;
        for (StartAndEndKey range : allRanges) {
            rangeLength += (Math.abs(range.getEnd() - range.getStart()) + 1);
        }

        List<StartAndEndKey> startAndEndKeys = new ArrayList<>();
        // rounding step value
        int gap = (int) Math.rint((rangeLength + 0.0) / startEndKeysCount);
        // generate start and end keys
        allRanges.forEach(range -> {
            int realGap = gap == 0 ? 1 : gap;
            for (char index = range.getStart(); index <= range.getEnd(); index += realGap) {
                char end = (index + realGap > range.getEnd()) ? range.getEnd() : (char) (index + realGap - 1);
                startAndEndKeys.add(new StartAndEndKey(index, end));
            }
        });

        return startAndEndKeys;
    }

    /**
     * Use client side mode, it will create a snapshot in HDFS for this table. Client Open the region to scan HDFS files
     * without sending scan requests to RegionServer.
     * Reasons for performance improvement:
     * 1. The scanning process decreased one time of network transmission. For scanning a large amount of data,
     *    network transmission takes a large amount of time, which may involve data serialization and deserialization
     *    overheads.
     * 2. RegionServer may become a bottleneck in normal scanning, but clientSide scanning does not.
     *
     * @param tupleDomain tupleDomain
     * @param tableHandle tableHandle
     * @return splits
     */
    private List<HBaseSplit> getSplitsForClientSide(TupleDomain<ColumnHandle> tupleDomain, HBaseTableHandle tableHandle)
    {
        List<HBaseSplit> splits = new ArrayList<>();
        TableName hbaseTableName = TableName.valueOf(tableHandle.getHbaseTableName().get());
        Map<Integer, List<Range>> ranges = predicateTransferToRanges(tupleDomain.getDomains().get());
        long startTime = System.currentTimeMillis();
        String snapshotName = tableHandle.getTableName() + "-" + startTime;

        try {
            if (!hbaseConnection.getHbaseAdmin().isTableAvailable(hbaseTableName)) {
                throw new PrestoException(HBaseErrorCode.UNEXPECTED_HBASE_ERROR,
                        format(tableHandle.getHbaseTableName().get() + " is not available."));
            }
            hbaseConnection.getHbaseAdmin().snapshot(snapshotName, hbaseTableName);
            LOG.info("Create Snapshot " + snapshotName + " finished, spend "
                        + (System.currentTimeMillis() - startTime) + " mill seconds.");
        }
        catch (Exception e) {
            if (e instanceof PrestoException) {
                throw new PrestoException(HBaseErrorCode.UNEXPECTED_HBASE_ERROR,
                        format(tableHandle.getHbaseTableName().get() + " is not available."));
            }
            int retryCreateSnapshotNumber = hbaseConnection.getHbaseConfig().getRetryCreateSnapshotNumber();
            for (int retry = 0; retry < retryCreateSnapshotNumber; retry++) {
                try {
                    Thread.sleep(1000);
                    snapshotName = tableHandle.getTableName() + "-" + System.currentTimeMillis();
                    hbaseConnection.getHbaseAdmin().snapshot(snapshotName, hbaseTableName);
                    LOG.info("Recreate snapshot success! snapshotName is " + snapshotName
                            + ", retried ：" + (retry + 1) + " times, using " + (System.currentTimeMillis() - startTime) + " mill seconds.");
                    break;
                }
                catch (Exception ee) {
                    if (retry == retryCreateSnapshotNumber - 1) {
                        LOG.error("Too many people create snapshot for the same table, maybe you should set 'hbase.client.side.snapshot.retry' more larger.");
                        LOG.error(ee, "Retry: create snapshot failed, snapshotName is " + snapshotName
                                + ", retried ：" + retryCreateSnapshotNumber + " times, track:" + ee.getMessage());
                    }
                }
            }
        }

        // get regions from snapshot
        List<RegionInfo> regionInfos = Utils.getRegionInfos(snapshotName, hbaseConnection);
        List<HostAddress> hostAddresses = new ArrayList<>();
        // create splits
        for (int index = 0; index < regionInfos.size(); index++) {
            // Client side region scanner using no startKey and endKey.
            splits.add(new HBaseSplit(tableHandle.getRowId(),
                    tableHandle,
                    hostAddresses,
                    null,
                    null,
                    ranges,
                    index,
                    false,
                    snapshotName));
        }

        printSplits("Client Side", splits);
        return splits;
    }

    /**
     * If the predicate of sql includes "rowKey='xxx'" or "rowKey in ('xxx','xxx')",
     * we can specify rowkey values in each split, then performance will be good.
     *
     * @param tupleDomain tupleDomain
     * @param tableHandle tableHandle
     * @return splits
     */
    private List<HBaseSplit> getSplitsForBatchGet(TupleDomain<ColumnHandle> tupleDomain, HBaseTableHandle tableHandle)
    {
        List<HBaseSplit> splits = new ArrayList<>();
        Domain rowIdDomain = null;
        Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().get();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            ColumnHandle handle = entry.getKey();
            if (handle instanceof HBaseColumnHandle) {
                HBaseColumnHandle columnHandle = (HBaseColumnHandle) handle;
                if (columnHandle.getOrdinal() == tableHandle.getRowIdOrdinal()) {
                    rowIdDomain = entry.getValue();
                }
            }
        }

        List<Range> rowIds = rowIdDomain != null ? rowIdDomain.getValues().getRanges().getOrderedRanges() : new ArrayList<>();
        int maxSplitSize;
        // Each split has at least 20 pieces of data, and the maximum number of splits is 30.
        if (rowIds.size() / Constants.BATCHGET_SPLIT_RECORD_COUNT > Constants.BATCHGET_SPLIT_MAX_COUNT) {
            maxSplitSize = rowIds.size() / Constants.BATCHGET_SPLIT_MAX_COUNT;
        }
        else {
            maxSplitSize = Constants.BATCHGET_SPLIT_RECORD_COUNT;
        }

        List<HostAddress> hostAddresses = new ArrayList<>();
        int rangeSize = rowIds.size();
        int currentIndex = 0;
        while (currentIndex < rangeSize) {
            int endIndex = rangeSize - currentIndex > maxSplitSize ? (currentIndex + maxSplitSize) : rangeSize;
            Map<Integer, List<Range>> splitRange = new HashMap<>();
            splitRange.put(tableHandle.getRowIdOrdinal(), rowIds.subList(currentIndex, endIndex));
            splits.add(new HBaseSplit(tableHandle.getRowId(), tableHandle, hostAddresses, null, null, splitRange, -1, false, null));
            currentIndex = endIndex;
        }

        printSplits("Batch Get", splits);
        return splits;
    }

    private Map<Integer, List<Range>> predicateTransferToRanges(Map<ColumnHandle, Domain> predicates)
    {
        Map<Integer, List<Range>> ranges = new HashMap<>();
        predicates
                .entrySet()
                .forEach(
                        entry -> {
                            ColumnHandle handle = entry.getKey();
                            if (handle instanceof HBaseColumnHandle) {
                                ranges.put(
                                        ((HBaseColumnHandle) handle).getOrdinal(),
                                        entry.getValue().getValues().getRanges().getOrderedRanges());
                            }
                        });
        return ranges;
    }

    private void printSplits(String scanType, List<HBaseSplit> splits)
    {
        LOG.info("The final split count is " + splits.size() + ".");
        for (HBaseSplit split : splits) {
            LOG.debug(scanType + ", Print Split: " + split.toString());
        }
    }
}
