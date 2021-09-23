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
package io.hetu.core.plugin.hbase.query;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.hetu.core.plugin.hbase.split.HBaseSplit;
import io.hetu.core.plugin.hbase.utils.Constants;
import io.hetu.core.plugin.hbase.utils.Utils;
import io.hetu.core.plugin.hbase.utils.serializers.HBaseRowSerializer;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

/**
 * HBaseRecordSet
 *
 * @since 2020-03-18
 */
public class HBaseRecordSet
        implements RecordSet
{
    private static final Logger LOG = Logger.get(HBaseRecordSet.class);

    private List<HBaseColumnHandle> columnHandles;

    private List<Type> columnTypes;

    private HBaseRowSerializer serializer;

    private String rowIdName;

    private ResultScanner scanner;

    private String[] fieldToColumnName;

    private HBaseSplit split;

    private Connection connection;

    private HBaseConnection hBaseConnection;

    private HBaseTableHandle table;

    private Scan scan;

    private String defaultValue;

    /**
     * constructor
     *
     * @param hbaseConn hbaseConn
     * @param session session
     * @param split split
     * @param table table
     * @param columnHandles columnHandles
     */
    public HBaseRecordSet(
            HBaseConnection hbaseConn,
            ConnectorSession session,
            HBaseSplit split,
            HBaseTableHandle table,
            List<HBaseColumnHandle> columnHandles)
    {
        requireNonNull(session, "session is null");
        rowIdName = table.getRowId();
        this.split = split;
        this.hBaseConnection = hbaseConn;
        this.connection = hbaseConn.getConn();
        this.serializer = HBaseRowSerializer.getSerializerInstance(table.getSerializerClassName());
        this.serializer.setRowIdName(rowIdName);
        this.columnHandles = columnHandles;
        this.table = table;
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (HBaseColumnHandle column : columnHandles) {
            types.add(column.getType());
        }
        this.columnTypes = types.build();
        this.defaultValue = hbaseConn.getDefaultValue();

        scan = new Scan();
        fieldToColumnName = new String[columnHandles.size()];
        for (int i = 0; i < this.columnHandles.size(); i++) {
            HBaseColumnHandle hc = columnHandles.get(i);
            fieldToColumnName[i] = hc.getName();

            if (!hc.getName().equals(rowIdName)) {
                scan.addColumn(
                        hc.getFamily().get().getBytes(Charset.forName("UTF-8")),
                        hc.getQualifier().get().getBytes(Charset.forName("UTF-8")));
                this.serializer.setMapping(hc.getName(), hc.getFamily().get(), hc.getQualifier().get());
            }
        }
        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= Integer.MAX_VALUE) {
            scan.setLimit((int) table.getLimit().getAsLong());
        }
        LOG.info("Worker handle split：" + split.toString());
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        try (Table hTable = connection.getTable(TableName.valueOf(table.getHbaseTableName().get()))) {
            if (Utils.isBatchGet(
                    this.split.getTableHandle().getConstraint(), this.split.getTableHandle().getRowIdOrdinal())) {
                return new HBaseGetRecordCursor(
                        columnHandles,
                        split,
                        connection,
                        serializer,
                        columnTypes,
                        rowIdName,
                        fieldToColumnName,
                        this.defaultValue);
            }
            else if (hBaseConnection.getHbaseConfig().isClientSideEnable()) {
                HBaseConfig hbaseConfig = hBaseConnection.getHbaseConfig();
                String hbaseRoot = hbaseConfig.getZkZnodeParent();
                Configuration conf = hBaseConnection.getConfiguration();
                Path root = new Path(hbaseRoot);
                FileSystem fs = hBaseConnection.getFileSystem();
                Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(split.getSnapshotName(), root);
                SnapshotProtos.SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
                SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
                TableDescriptor htd = manifest.getTableDescriptor();
                List<RegionInfo> regionInfos = Utils.getRegionInfoFromManifest(manifest);

                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                setAttributeToScan(false);
                RegionInfo regionInfo = regionInfos.get(split.getRegionIndex());
                scan.withStartRow(regionInfo.getStartKey()).withStopRow(regionInfo.getEndKey());
                scanner = new ClientSideRegionScanner(conf, fs, root, htd, regionInfo, scan, null);
                Thread.currentThread().setContextClassLoader(classLoader);
                return new HBaseRecordCursor(
                        columnHandles, columnTypes, serializer, scanner, fieldToColumnName, rowIdName, this.defaultValue);
            }
            else {
                setAttributeToScan(true);
                scanner = hTable.getScanner(scan);
                return new HBaseRecordCursor(
                        columnHandles, columnTypes, serializer, scanner, fieldToColumnName, rowIdName, this.defaultValue);
            }
        }
        catch (IOException e) {
            LOG.error("HBaseRecordSet : getScanner failed... cause by %s", e.getMessage());
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (Exception ex) {
                    LOG.error(ex.getMessage(), ex);
                }
            }
            return null;
        }
    }

    /**
     * getHBaseTableHandle
     *
     * @return HBaseTableHandle
     */
    public HBaseTableHandle getHBaseTableHandle()
    {
        return this.table;
    }

    /**
     * set column/filters/attributes to scan
     */
    public void setAttributeToScan(boolean setFilter)
    {
        columnHandles.stream()
                .forEach(
                        columnHandle -> {
                            if (this.rowIdName == null
                                    || !this.rowIdName.equals(columnHandle.getColumnName())) {
                                scan.addColumn(
                                        Bytes.toBytes(columnHandle.getFamily().get()),
                                        Bytes.toBytes(columnHandle.getQualifier().get()));
                            }
                        });

        if (setFilter) {
            Map<Integer, List<Range>> domainMap = this.split.getRanges();
            FilterList filters = getFiltersFromDomains(domainMap);
            if (filters.getFilters().size() != 0) {
                scan.setFilter(filters);
            }
        }

        if (split.getStartRow() != null && !split.getStartRow().isEmpty()) {
            scan.withStartRow(Bytes.toBytes(split.getStartRow()));
        }
        if (split.getEndRow() != null && !split.getEndRow().isEmpty()) {
            scan.withStopRow(Bytes.toBytes(split.getEndRow()));
        }

        scan.setCaching(Constants.SCAN_CACHING_SIZE);
        scan.setLoadColumnFamiliesOnDemand(true);
        scan.setCacheBlocks(true);
    }

    /**
     * transform domain to filters
     *
     * @param domainMap domains
     * @return filterList
     */
    public FilterList getFiltersFromDomains(Map<Integer, List<Range>> domainMap)
    {
        FilterList andFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        // select count(*) / count(rowKey) / rowKey from table_xxx;
        if ((this.columnHandles.size() == 1
                && this.columnHandles.get(0).getColumnName().equals(this.split.getRowKeyName()))
                || this.columnHandles.size() == 0) {
            andFilters.addFilter(new FirstKeyOnlyFilter());
            andFilters.addFilter(new KeyOnlyFilter());
        }

        if (domainMap == null || domainMap.isEmpty()) {
            return andFilters;
        }

        domainMap.entrySet().stream()
                .forEach(
                        entry -> {
                            Integer columnId = entry.getKey();
                            List<Range> ranges = entry.getValue();
                            HBaseColumnHandle columnHandle =
                                    columnHandles.stream()
                                            .filter(col -> checkPredicateType(col) && columnId == col.getOrdinal())
                                            .findAny()
                                            .orElse(null);
                            if (ranges == null || columnHandle == null) {
                                return;
                            }

                            // inFilters: put "="
                            List<Filter> inFilters = new ArrayList<>();
                            // filters: put "<" "<=" ">" ">="
                            List<Filter> filters = new ArrayList<>();

                            getFiltersFromRanges(ranges, columnHandle, andFilters, inFilters, filters);

                            // operate is IN / =
                            if (inFilters.size() != 0) {
                                FilterList columnFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE, inFilters);
                                andFilters.addFilter(columnFilter);
                            }
                            // NOT IN (3,4) become "id < 3" or "3 < id < 4" or "id > 4"
                            // != 3 become "id < 3" or "id > 3"
                            if (ranges.size() > 1 && filters.size() != 0) {
                                FilterList orFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
                                andFilters.addFilter(orFilter);
                            }
                            if (ranges.size() <= 1 && filters.size() != 0) {
                                FilterList andFilter = new FilterList(filters);
                                andFilters.addFilter(andFilter);
                            }
                            // operator IS NULL
                            if (ranges.isEmpty()) {
                                boolean isKey = (this.split.getRowKeyName().equals(columnHandle.getName()));
                                andFilters.addFilter(
                                        columnOrKeyFilter(columnHandle, null, CompareOperator.EQUAL, isKey));
                            }
                        });

        return andFilters;
    }

    private boolean checkPredicateType(HBaseColumnHandle columnHandle)
    {
        Type type = columnHandle.getType();
        // type list support to push down
        if (type.equals(BOOLEAN)) {
            return true;
        }
        else if (type.equals(DOUBLE)) {
            return true;
        }
        else if (type.equals(BIGINT)) {
            return true;
        }
        else if (type.equals(INTEGER)) {
            return true;
        }
        else if (type.equals(SMALLINT)) {
            return true;
        }
        else if (type.equals(TINYINT)) {
            return true;
        }
        else if (type instanceof VarcharType) {
            return true;
        }
        else {
            return false;
        }
    }

    private String getRangeValue(Object object)
    {
        String value;
        if (object instanceof Slice) {
            value = ((Slice) object).toStringUtf8();
        }
        else {
            value = object.toString();
        }

        return value;
    }

    private void getFiltersFromRanges(
            List<Range> ranges,
            HBaseColumnHandle columnHandle,
            FilterList andFilters,
            List<Filter> inFilters,
            List<Filter> filters)
    {
        final int filterSize = 2;
        boolean isKey = (this.split.getRowKeyName().equals(columnHandle.getName()));

        for (Range range : ranges) {
            if (range.isSingleValue()) {
                String columnValue = String.valueOf(getRangeValue(range.getSingleValue()));
                inFilters.add(columnOrKeyFilter(columnHandle, columnValue, CompareOperator.EQUAL, isKey));
            }
            else {
                String columnValueLower = null;
                String columnValueHigher = null;
                int count = 0;
                if (!range.getLow().isLowerUnbounded()) {
                    columnValueLower =
                            (range.getLow().getValueBlock().isPresent())
                                    ? String.valueOf(getRangeValue(range.getLow().getValue()))
                                    : null;
                    addFilterByLowerBound(range.getLow().getBound(), columnValueLower, isKey, columnHandle, filters);
                    count++;
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    columnValueHigher =
                            (range.getHigh().getValueBlock().isPresent())
                                    ? String.valueOf(getRangeValue(range.getHigh().getValue()))
                                    : null;
                    addFilterByHigherBound(range.getHigh().getBound(), columnValueHigher, isKey, columnHandle, filters);
                    count++;
                }
                // is not null
                if (columnValueLower == null && columnValueHigher == null) {
                    andFilters.addFilter(
                            columnOrKeyFilter(columnHandle, null, CompareOperator.NOT_EQUAL, isKey));
                }
                if (count == filterSize) {
                    Filter left = filters.remove(filters.size() - 1);
                    Filter right = filters.remove(filters.size() - 1);
                    FilterList andFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                    andFilter.addFilter(left);
                    andFilter.addFilter(right);
                    filters.add(andFilter);
                }
            }
        }
    }

    /**
     * construct filter
     *
     * @param columnHandle columnHandle
     * @param columnValue columnValue
     * @param operator operator
     * @param isKey isKey
     * @return Filter
     */
    public Filter columnOrKeyFilter(
            HBaseColumnHandle columnHandle, String columnValue, CompareOperator operator, boolean isKey)
    {
        byte[] values = (columnValue != null) ? Bytes.toBytes(columnValue) : null;

        if (isKey) {
            return new RowFilter(operator, new BinaryComparator(values));
        }
        else {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    Bytes.toBytes(columnHandle.getFamily().get()),
                    Bytes.toBytes(columnHandle.getQualifier().get()),
                    operator,
                    values);
            return filter;
        }
    }

    /**
     * @param boundEnum ABOVE/EXACTLY
     * @param columnValueLower value
     * @param isKey is row key
     * @param columnHandle columnHandle
     * @param filters filters
     */
    public void addFilterByLowerBound(
            Marker.Bound boundEnum,
            String columnValueLower,
            boolean isKey,
            HBaseColumnHandle columnHandle,
            List<Filter> filters)
    {
        switch (boundEnum) {
            // >
            case ABOVE:
                filters.add(columnOrKeyFilter(columnHandle, columnValueLower, CompareOperator.GREATER, isKey));
                break;
            // >=
            case EXACTLY:
                filters.add(
                        columnOrKeyFilter(
                                columnHandle, columnValueLower, CompareOperator.GREATER_OR_EQUAL, isKey));
                break;
            default:
                throw new AssertionError("Unhandled bound: " + boundEnum);
        }
    }

    /**
     * @param boundEnum BELOW/EXACTLY
     * @param columnValueHigher value
     * @param isKey is row key
     * @param columnHandle columnHandle
     * @param filters filters
     */
    public void addFilterByHigherBound(
            Marker.Bound boundEnum,
            String columnValueHigher,
            boolean isKey,
            HBaseColumnHandle columnHandle,
            List<Filter> filters)
    {
        switch (boundEnum) {
            // <
            case BELOW:
                filters.add(columnOrKeyFilter(columnHandle, columnValueHigher, CompareOperator.LESS, isKey));
                break;
            // <=
            case EXACTLY:
                filters.add(
                        columnOrKeyFilter(
                                columnHandle, columnValueHigher, CompareOperator.LESS_OR_EQUAL, isKey));
                break;
            default:
                throw new AssertionError("Unhandled bound: " + boundEnum);
        }
    }
}
