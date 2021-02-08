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
package io.hetu.core.plugin.hbase.split;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.hetu.core.plugin.hbase.utils.Constants;
import io.hetu.core.plugin.hbase.utils.HBaseErrorCode;
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
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private final HBaseConnection hBaseConnection;

    @Inject
    public HBaseSplitManager(HBaseConnection hBaseConnection)
    {
        this.hBaseConnection = hBaseConnection;
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
        else {
            splits = getSplitsForScan(tupleDomain, tableHandle);
        }

        return new FixedSplitSource(splits);
    }

    private List<HBaseSplit> getSplitsForScan(TupleDomain<ColumnHandle> tupleDomain, HBaseTableHandle tableHandle)
    {
        List<HBaseSplit> splits = new ArrayList<>();
        Pair<byte[][], byte[][]> startEndKeys = null;
        TableName hbaseTableName = TableName.valueOf(tableHandle.getHbaseTableName().get());

        try {
            if (hBaseConnection.getHbaseAdmin().getTableDescriptor(hbaseTableName) != null) {
                RegionLocator regionLocator =
                        hBaseConnection.getConn().getRegionLocator(hbaseTableName);
                startEndKeys = regionLocator.getStartEndKeys();
            }
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(
                    HBaseErrorCode.HBASE_TABLE_DNE,
                    format(
                            "table %s not found, maybe deleted by other user", tableHandle.getHbaseTableName().get()));
        }
        catch (IOException e) {
            LOG.error(e.getMessage());
        }

        Map<Integer, List<Range>> ranges = new HashMap<>();
        Map<ColumnHandle, Domain> predicates = tupleDomain.getDomains().get();
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

        List<HostAddress> hostAddresses = new ArrayList<>();
        if (startEndKeys == null) {
            throw new NullPointerException("null pointer found when getting splits for scan");
        }
        for (int i = 0; i < startEndKeys.getFirst().length; i++) {
            String startRow = new String(startEndKeys.getFirst()[i]);
            String endRow = new String(startEndKeys.getSecond()[i]);
            splits.add(
                    new HBaseSplit(
                            tableHandle.getRowId(), tableHandle, hostAddresses, startRow, endRow, ranges, null, false));
        }

        return splits;
    }

    private List<HBaseSplit> getSplitsForBatchGet(TupleDomain<ColumnHandle> tupleDomain, HBaseTableHandle table)
    {
        List<HBaseSplit> splits = new ArrayList<>();
        Domain rowIdDomain = null;
        Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().get();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            ColumnHandle handle = entry.getKey();
            if (handle instanceof HBaseColumnHandle) {
                HBaseColumnHandle columnHandle = (HBaseColumnHandle) handle;
                if (columnHandle.getOrdinal() == table.getRowIdOrdinal()) {
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
            int endIndex = rangeSize - currentIndex > maxSplitSize ? (currentIndex + currentIndex) : rangeSize;
            Map<Integer, List<Range>> splitRange = new HashMap<>();
            splitRange.put(table.getRowIdOrdinal(), rowIds.subList(currentIndex, endIndex));
            splits.add(new HBaseSplit(table.getRowId(), table, hostAddresses, null, null, splitRange, null, false));
            currentIndex += endIndex - currentIndex;
        }

        for (HBaseSplit split : splits) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Print Split: " + split.toString());
            }
        }

        return splits;
    }
}
