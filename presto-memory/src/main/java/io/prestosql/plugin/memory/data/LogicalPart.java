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
package io.prestosql.plugin.memory.data;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeUtil;
import io.prestosql.plugin.memory.ColumnInfo;
import io.prestosql.plugin.memory.MemoryColumnHandle;
import io.prestosql.plugin.memory.SortingColumn;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeUtils;
import io.prestosql.spi.util.BloomFilter;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static java.util.Objects.requireNonNull;

public class LogicalPart
        implements Serializable
{
    private static final long serialVersionUID = -8601397465573888504L;
    private static final Logger LOG = Logger.get(LogicalPart.class);
    private static final JsonCodec<TypeSignature> TYPE_SIGNATURE_JSON_CODEC = JsonCodec.jsonCodec(TypeSignature.class);
    private static final String TABLE_DATA_FOLDER = "data";

    enum ProcessingState
    {
        NOT_STARTED, IN_PROGRESS, COMPLETE
    }

    private long rows;
    private long byteSize;

    private final AtomicReference<ProcessingState> processingState = new AtomicReference<>(ProcessingState.NOT_STARTED);
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private final List<Integer> indexChannels;
    private final long maxLogicalPartBytes;
    private final int maxPageSizeBytes;
    private final int splitNum;
    private final int logicalPartNum;
    private final boolean compressionEnabled;

    // indexes
    private final TreeMap<Comparable, List<Integer>> indexedPagesMap = new TreeMap<>();
    private final Map<Integer, BloomFilter> indexChannelFilters = new HashMap<>();
    private final Map<Integer, Map.Entry<Comparable, Comparable>> columnMinMax = new HashMap<>();

    private transient Path tableDataRoot;
    private transient PagesSerde pagesSerde;
    private transient PageSorter pageSorter;
    private transient List<TypeSignature> typeSignatures;
    private transient List<Type> types;
    private transient List<Page> pages;

    public LogicalPart(
            List<ColumnInfo> columns,
            List<SortingColumn> sortedBy,
            List<String> indexColumns,
            Path tableDataRoot,
            PageSorter pageSorter,
            long maxLogicalPartBytes,
            int maxPageSizeBytes,
            TypeManager typeManager,
            PagesSerde pagesSerde,
            int splitNum,
            int logicalPartNum,
            boolean compressionEnabled)
    {
        this.tableDataRoot = tableDataRoot;
        this.splitNum = splitNum;
        this.logicalPartNum = logicalPartNum;
        this.pages = new ArrayList<>();
        this.maxLogicalPartBytes = maxLogicalPartBytes;
        this.maxPageSizeBytes = maxPageSizeBytes;
        this.compressionEnabled = compressionEnabled;
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(sortedBy, "sortedBy is null");

        types = new ArrayList<>();
        for (ColumnInfo column : columns) {
            types.add(column.getType(typeManager));
        }

        sortChannels = new ArrayList<>();
        sortOrders = new ArrayList<>();
        indexChannels = new ArrayList<>();

        for (SortingColumn sortingColumn : sortedBy) {
            String sortColumnName = sortingColumn.getColumnName();
            for (ColumnInfo column : columns) {
                if (column.getName().equalsIgnoreCase(sortColumnName)) {
                    sortChannels.add(column.getIndex());
                    sortOrders.add(sortingColumn.getOrder());
                    indexChannels.add(column.getIndex());
                    break;
                }
            }
        }

        for (String indexColumn : indexColumns) {
            for (ColumnInfo column : columns) {
                if (column.getName().equalsIgnoreCase(indexColumn)) {
                    indexChannels.add(column.getIndex());
                }
            }
        }

        this.processingState.set(ProcessingState.NOT_STARTED);
    }

    void restoreTransientObjects(PageSorter pageSorter, TypeManager typeManager, PagesSerde pagesSerde, Path tableDataRoot)
    {
        this.pageSorter = pageSorter;
        this.types = new ArrayList<>(typeSignatures.size());
        this.pagesSerde = pagesSerde;
        this.tableDataRoot = tableDataRoot;
        for (TypeSignature signature : typeSignatures) {
            types.add(typeManager.getType(signature));
        }
    }

    void add(Page page)
    {
        if (!canAdd()) {
            throw new RuntimeException("This LogicalPart can no longer be modified, create a new one.");
        }

        pages.add(page);
        rows += page.getPositionCount();
        byteSize += page.getRetainedSizeInBytes();
    }

    List<Page> getPages()
    {
        if (pages == null) {
            try {
                readPages();
            }
            catch (Exception e) {
                LOG.error("Failed to load pages from " + getPageFileName(), e);
//                throw new UncheckedIOException("Unable to load pages from disk", e);
            }
        }
        return pages;
    }

    /**
     * The Domains in TupleDomain are all ANDed together,
     * this means if any one of the Domains don't match
     * the LogicalPart can be skipped.
     * <p>
     * The following indexes are checked:
     * 1. minmax - if Domain lookup value is outside minmax range, skip LogicalPart
     * 2. bloom - if Domain lookup value is not found in bloom filter, skip LogicalPart
     * 3. sparse - check the lookup value in sparse index, if exact match is not found,
     * there may still be a match in the key before. for example
     * 100 -> (100,101,150)
     * 200 -> (200,250)
     * <p>
     * if the lookup value is 150, the sparse index returns false bc 150 is not found but
     * 150 is still in the list with key 100
     *
     * @param predicate
     * @return
     */
    List<Page> getPages(TupleDomain<ColumnHandle> predicate)
    {
        if (processingState.get() != ProcessingState.COMPLETE) {
            return getPages();
        }

        Map<Integer, Domain> allIdxChannelsToDomainMap = new HashMap<>();
        Map<Integer, Domain> bloomIdxChannelsToDomainMap = new HashMap<>();
        Map<Integer, Domain> sparseIdxChannelsToDomainMap = new HashMap<>();
        for (Map.Entry<ColumnHandle, Domain> e : predicate.getDomains().orElse(Collections.emptyMap()).entrySet()) {
            int expressionColumnIndex = ((MemoryColumnHandle) e.getKey()).getColumnIndex();

            allIdxChannelsToDomainMap.put(expressionColumnIndex, e.getValue());

            if (indexChannels.contains(expressionColumnIndex)) {
                bloomIdxChannelsToDomainMap.put(expressionColumnIndex, e.getValue());
            }

            if (sortChannels.contains(expressionColumnIndex)) {
                sparseIdxChannelsToDomainMap.put(expressionColumnIndex, e.getValue());
            }
        }

        // start filtering
        if (bloomIdxChannelsToDomainMap.isEmpty() && sparseIdxChannelsToDomainMap.isEmpty()) {
            return getPages();
        }

        return getPages(allIdxChannelsToDomainMap, bloomIdxChannelsToDomainMap, sparseIdxChannelsToDomainMap);
    }

    List<Page> getPages(
            Map<Integer, Domain> allIdxChannelsToDomainMap,
            Map<Integer, Domain> bloomIdxChannelsToDomainMap,
            Map<Integer, Domain> sparseIdxChannelsToDomainMap)
    {
        // check minmax filter
        for (Map.Entry<Integer, Domain> e : allIdxChannelsToDomainMap.entrySet()) {
            Domain columnPredicate = e.getValue();
            int expressionColumnIndex = e.getKey();
            if (columnMinMax.containsKey(expressionColumnIndex)) {
                List<Range> ranges = ((SortedRangeSet) (columnPredicate.getValues())).getOrderedRanges();
                int failedLookups = 0;
                for (Range range : ranges) {
                    if (range.isSingleValue()) {
                        Object lookupValue = getNativeValue(range.getSingleValue());
                        if (lookupValue instanceof Comparable) {
                            Comparable comparableLookupValue = (Comparable) lookupValue;
                            Map.Entry<Comparable, Comparable> columnMinMaxEntry = columnMinMax.get(e.getKey());
                            Comparable min = columnMinMaxEntry.getKey();
                            Comparable max = columnMinMaxEntry.getValue();

                            if (comparableLookupValue.compareTo(min) < 0 || comparableLookupValue.compareTo(max) > 0) {
                                // lookup value is outside minmax range, skip logicalpart
                                failedLookups++;
                            }
                        }
                    }
                    else {
                        // <, <=, >=, >, BETWEEN
                        boolean highBoundless = range.getHigh().isUpperUnbounded();
                        boolean lowBoundless = range.getLow().isLowerUnbounded();
                        Map.Entry<Comparable, Comparable> columnMinMaxEntry = columnMinMax.get(e.getKey());
                        Comparable min = columnMinMaxEntry.getKey();
                        Comparable max = columnMinMaxEntry.getValue();
                        if (highBoundless && !lowBoundless) {
                            // >= or >
                            Object lowLookupValue = range.getLow().getValue();
                            if (lowLookupValue instanceof Comparable) {
                                Comparable lowComparableLookupValue = (Comparable) lowLookupValue;
                                boolean inclusive = range.getLow().getBound().equals(Marker.Bound.EXACTLY);
                                if (inclusive) {
                                    if (lowComparableLookupValue.compareTo(max) > 0) {
                                        // lookup value is outside minmax range, skip logicalpart
                                        failedLookups++;
                                    }
                                }
                                else {
                                    if (lowComparableLookupValue.compareTo(max) >= 0) {
                                        // lookup value is outside minmax range, skip logicalpart
                                        failedLookups++;
                                    }
                                }
                            }
                        }
                        else if (!highBoundless && lowBoundless) {
                            // <= or <
                            Object highLookupValue = range.getHigh().getValue();
                            if (highLookupValue instanceof Comparable) {
                                Comparable highComparableLookupValue = (Comparable) highLookupValue;
                                boolean inclusive = range.getHigh().getBound().equals(Marker.Bound.EXACTLY);
                                if (inclusive) {
                                    if (highComparableLookupValue.compareTo(min) < 0) {
                                        // lookup value is outside minmax range, skip logicalpart
                                        failedLookups++;
                                    }
                                }
                                else {
                                    if (highComparableLookupValue.compareTo(min) <= 0) {
                                        // lookup value is outside minmax range, skip logicalpart
                                        failedLookups++;
                                    }
                                }
                            }
                        }
                        else if (!highBoundless && !lowBoundless) {
                            // BETWEEN
                            Object lowLookupValue = range.getLow().getValue();
                            Object highLookupValue = range.getHigh().getValue();
                            if (lowLookupValue instanceof Comparable && highLookupValue instanceof Comparable) {
                                Comparable lowComparableLookupValue = (Comparable) lowLookupValue;
                                Comparable highComparableLookupValue = (Comparable) highLookupValue;
                                if (lowComparableLookupValue.compareTo(max) > 0 || highComparableLookupValue.compareTo(min) < 0) {
                                    // lookup value is outside minmax range, skip logicalpart
                                    failedLookups++;
                                }
                            }
                        }
                    }
                }
                if (failedLookups == ranges.size()) {
                    return Collections.emptyList();
                }
            }
        }

        // next check bloom filter, if any domain returns false this LogicalPart did not have a match
        boolean match = true;
        for (Map.Entry<Integer, Domain> e : bloomIdxChannelsToDomainMap.entrySet()) {
            Domain columnPredicate = e.getValue();
            int expressionColumnIndex = e.getKey();

            List<Range> ranges = ((SortedRangeSet) (columnPredicate.getValues())).getOrderedRanges();

            // bloom only supports equality e.g. c=1
            if (ranges.size() != 1 || !ranges.get(0).isSingleValue()) {
                continue;
            }

            Object lookupValue = getNativeValue(ranges.get(0).getSingleValue());
            BloomFilter filter = indexChannelFilters.get(expressionColumnIndex);
            if (!testFilter(filter, lookupValue)) {
                match = false;
                break;
            }
        }

        // no match with bloom indexed columns
        if (!match) {
            return Collections.emptyList();
        }

        // apply sparse index
        // TODO: currently only one sort column is supported so if there's an matching sparse index it will automatically be on that column
        for (Map.Entry<Integer, Domain> e : sparseIdxChannelsToDomainMap.entrySet()) {
            Domain columnPredicate = e.getValue();
            int expressionColumnIndex = e.getKey();

            List<Range> ranges = ((SortedRangeSet) (columnPredicate.getValues())).getOrderedRanges();

            // assume pages were sorted as follows:
            // [a->[{a,a}, {a,a}, {a,b}], b->[{b,b}, {b,b}, {b}]
            // i.e. a has three pages, b has 3 pages
            // and the expression is column=b
            // first lookup b for an exact match, which will return the second page
            // however, there is also a "b" in the last page of "a" list
            // this must be returned also

            Set<Integer> result = new HashSet<>();
            for (Range range : ranges) {
                if (range.isSingleValue()) {
                    // unique value(for example: id=1, id in (1) (IN operator gives single exact values one by one)), bound: EXACTLY
                    Object lookupValue = getNativeValue(range.getSingleValue());
                    if (!(lookupValue instanceof Comparable)) {
                        LOG.warn("Lookup value is not Comparable. Sparse index could not be queried.");
                        return getPages();
                    }
                    result.addAll(indexedPagesMap.getOrDefault(lookupValue, new ArrayList<>()));

                    Integer additionalPageIdx = getLowerPageIndex((Comparable) lookupValue, (Comparable) lookupValue, true, null, false);
                    if (additionalPageIdx != null) {
                        result.add(additionalPageIdx);
                    }
                    continue;
                }
                else {
                    // <, <=, >=, >, BETWEEN
                    boolean highBoundless = range.getHigh().isUpperUnbounded();
                    boolean lowBoundless = range.getLow().isLowerUnbounded();
                    boolean fromInclusive = range.getLow().getBound().equals(Marker.Bound.EXACTLY);
                    boolean toInclusive = range.getHigh().getBound().equals(Marker.Bound.EXACTLY);

                    NavigableMap<Comparable, List<Integer>> navigableMap = null;
                    Comparable low = null;
                    Comparable high = null;
                    if (highBoundless && !lowBoundless) {
                        // >= or >
                        if (!(range.getLow().getValue() instanceof Comparable)) {
                            LOG.warn("Lookup value is not Comparable. Sparse index could not be queried.");
                            return getPages();
                        }
                        low = (Comparable) range.getLow().getValue();
                        high = (Comparable) indexedPagesMap.lastKey();
                        if (low.compareTo(high) > 0) {
                            navigableMap = Collections.emptyNavigableMap();
                        }
                        else {
                            navigableMap = indexedPagesMap.subMap(low, fromInclusive, high, true);
                        }
                    }
                    else if (!highBoundless && lowBoundless) {
                        // <= or <
                        if (!(range.getHigh().getValue() instanceof Comparable)) {
                            LOG.warn("Lookup value is not Comparable. Sparse index could not be queried.");
                            return getPages();
                        }
                        low = (Comparable) indexedPagesMap.firstKey();
                        high = (Comparable) range.getHigh().getValue();
                        toInclusive = range.getHigh().getBound().equals(Marker.Bound.EXACTLY);
                        if (low.compareTo(high) > 0) {
                            navigableMap = Collections.emptyNavigableMap();
                        }
                        else {
                            navigableMap = indexedPagesMap.subMap(low, true, high, toInclusive);
                        }
                    }
                    else if (!highBoundless && !lowBoundless) {
                        // BETWEEN, non-inclusive range < && >
                        if (!(range.getLow().getValue() instanceof Comparable || range.getHigh().getValue() instanceof Comparable)) {
                            LOG.warn("Lookup value is not Comparable. Sparse index could not be queried.");
                            return getPages();
                        }
                        low = min((Comparable) range.getHigh().getValue(), (Comparable) range.getLow().getValue());
                        high = max((Comparable) range.getHigh().getValue(), (Comparable) range.getLow().getValue());
                        navigableMap = indexedPagesMap.subMap(low, fromInclusive, high, toInclusive);
                    }
                    else {
                        return getPages();
                    }

                    for (Map.Entry entry : navigableMap.entrySet()) {
                        result.addAll((Collection<? extends Integer>) entry.getValue());
                    }
                    if (!lowBoundless) {
                        Comparable lowestSparseIdxInDom = null;
                        if (!navigableMap.isEmpty()) {
                            lowestSparseIdxInDom = navigableMap.firstKey();
                        }
                        Integer additionalResultIdx = getLowerPageIndex(lowestSparseIdxInDom, low, fromInclusive, high, toInclusive);

                        if (additionalResultIdx != null) {
                            result.add(additionalResultIdx);
                        }
                    }
                }
            }

            List<Page> resultPageList = new ArrayList<>();
            for (Integer idx : result) {
                resultPageList.add(getPages().get(idx));
            }
            return resultPageList;
        }

        return null;
    }

    private Integer getLowerPageIndex(Comparable lowestInDom, Comparable lowBound, boolean includeLowBound, Comparable highBound, boolean includeHighBound)
    {
        Map.Entry<Comparable, List<Integer>> lowerSparseEntry;
        if (lowestInDom != null) {
            lowerSparseEntry = indexedPagesMap.lowerEntry(lowestInDom);
        }
        else {
            lowerSparseEntry = indexedPagesMap.floorEntry(lowBound);
        }

        if (lowerSparseEntry == null) {
            return null;
        }

        List<Integer> lowerPages = lowerSparseEntry.getValue();
        Integer lastPageIdx = lowerPages.get(lowerPages.size() - 1);
        Page lastPage = getPages().get(lastPageIdx);

        Object lastPageLastValue = getNativeValue(types.get(sortChannels.get(0)), lastPage.getBlock(sortChannels.get(0)), lastPage.getPositionCount() - 1);
        if (!(lastPageLastValue instanceof Comparable)) {
            return lastPageIdx;
        }

        int comp = ((Comparable) lastPageLastValue).compareTo(lowBound);
        if (comp > 0 || (comp == 0 && includeLowBound)) {
            return lastPageIdx;
        }

        return null;
    }

    long getRows()
    {
        return rows;
    }

    void process()
    {
        if (processingState.get() != ProcessingState.NOT_STARTED) {
            return;
        }

        processingState.set(ProcessingState.IN_PROGRESS);

        // sort and create sparse index
        if (!sortChannels.isEmpty()) {
            SortBuffer sortBuffer = new SortBuffer(
                    new DataSize(10, DataSize.Unit.GIGABYTE),
                    types,
                    sortChannels,
                    sortOrders,
                    pageSorter,
                    maxPageSizeBytes);
            pages.stream().forEach(sortBuffer::add);
            List<Page> sortedPages = new ArrayList<>();
            sortBuffer.flushTo(sortedPages::add);

            // create index
            int newRowCount = 0;
            long newByteSize = 0;
            for (int i = 0; i < sortedPages.size(); i++) {
                Page page = sortedPages.get(i);
                newByteSize += page.getRetainedSizeInBytes();
                newRowCount += page.getPositionCount();
                Object value = getNativeValue(types.get(sortChannels.get(0)), page.getBlock(sortChannels.get(0)), 0);
                if (value != null) {
                    if (!(value instanceof Comparable)) {
                        throw new RuntimeException(String.format("Unable to create sparse index for channel %d, type is not Comparable.", sortChannels.get(0)));
                    }
                    indexedPagesMap.computeIfAbsent((Comparable) value, e -> new LinkedList<>()).add(i);
                }
            }

            if (newRowCount != rows) {
                throw new RuntimeException("Pages mismatch while processing");
            }

            // create minmax index for sort column
            Page firstPage = sortedPages.get(0);
            Page lastPage = sortedPages.get(sortedPages.size() - 1);

            Object minValue = getNativeValue(types.get(sortChannels.get(0)), firstPage.getBlock(sortChannels.get(0)), 0);
            Object maxValue = getNativeValue(types.get(sortChannels.get(0)), lastPage.getBlock(sortChannels.get(0)), lastPage.getPositionCount() - 1);

            if (minValue instanceof Comparable && maxValue instanceof Comparable) {
                columnMinMax.put(sortChannels.get(0), new AbstractMap.SimpleEntry<>((Comparable) minValue, (Comparable) maxValue));
            }

            this.byteSize = newByteSize;
            this.pages.clear(); // help triggering GC of old pages
            this.pages = sortedPages;
        }

        // create bloom index on index columns
        for (Integer indexChannel : indexChannels) {
            Set<Object> values = new HashSet<>();
            for (Page page : pages) {
                for (int i = 0; i < page.getPositionCount(); i++) {
                    Object value = getNativeValue(types.get(indexChannel), page.getBlock(indexChannel), i);
                    if (value != null) {
                        values.add(value);
                    }
                }
            }

            BloomFilter filter = new BloomFilter(values.size(), 0.05);
            boolean unsupportedValue = false;
            Comparable min = null;
            Comparable max = null;
            for (Object value : values) {
                if (value instanceof Comparable) {
                    Comparable comparableValue = (Comparable) value;
                    min = min(min, comparableValue);
                    max = max(max, comparableValue);
                }

                if (!addToFilter(filter, value)) {
                    LOG.warn("Unsupported index column type %s", value.getClass().getSimpleName());
                    unsupportedValue = true;
                    min = null;
                    max = null;
                    break;
                }
            }

            if (min != null && max != null) {
                columnMinMax.put(indexChannel, new AbstractMap.SimpleEntry<>(min, max));
            }

            if (unsupportedValue) {
                continue;
            }

            indexChannelFilters.put(indexChannel, filter);
        }

        try {
            writePages();
        }
        catch (Exception e) {
            LOG.error("Error spilling LogicalPart " + getPageFileName() + " to disk. Restoring will be unavailable.", e);
        }
        this.processingState.set(ProcessingState.COMPLETE);
    }

    private String getPageFileName()
    {
        return "split" + splitNum + "lp" + logicalPartNum;
    }

    private synchronized void readPages()
            throws IOException
    {
        if (pages != null) {
            return;
        }
        long start = System.currentTimeMillis();
        Path pagesFile = tableDataRoot.resolve(TABLE_DATA_FOLDER).resolve(getPageFileName());
        try (InputStream inputStream = Files.newInputStream(pagesFile)) {
            try (InputStream inputStreamToUse = compressionEnabled ? new GZIPInputStream(inputStream) : inputStream) {
                SliceInput sliceInput = new InputStreamSliceInput(inputStreamToUse);
                pages = new ArrayList<>();
                PagesSerdeUtil.readPages(pagesSerde, sliceInput).forEachRemaining(pages::add);
            }
        }
        long dur = System.currentTimeMillis() - start;
        LOG.debug("[Load] %s completed. Time elapsed: %dms", pagesFile.toString(), dur);
    }

    private synchronized void writePages()
            throws IOException
    {
        long start = System.currentTimeMillis();
        Path pagesFile = tableDataRoot.resolve(TABLE_DATA_FOLDER).resolve(getPageFileName());
        if (!Files.exists(pagesFile.getParent())) {
            Files.createDirectories(pagesFile.getParent());
        }
        try (OutputStream outputStream = Files.newOutputStream(pagesFile)) {
            try (OutputStream outputStreamToUse = compressionEnabled ? new GZIPOutputStream(outputStream) : outputStream) {
                SliceOutput sliceOutput = new OutputStreamSliceOutput(outputStreamToUse);
                PagesSerdeUtil.writePages(pagesSerde, sliceOutput, pages.iterator());
                sliceOutput.flush();
            }
        }
        long dur = System.currentTimeMillis() - start;
        LOG.debug("[Spill] %s completed. Time elapsed: %dms", pagesFile.toString(), dur);
    }

    private Comparable min(Comparable c1, Comparable c2)
    {
        if (c1 == null && c2 != null) {
            return c2;
        }
        else if (c1 != null && c2 == null) {
            return c1;
        }
        else if (c1 != null && c2 != null) {
            if (c1.compareTo(c2) <= 0) {
                return c1;
            }
            else {
                return c2;
            }
        }
        else {
            return null;
        }
    }

    private Comparable max(Comparable c1, Comparable c2)
    {
        if (c1 == null && c2 != null) {
            return c2;
        }
        else if (c1 != null && c2 == null) {
            return c1;
        }
        else if (c1 != null && c2 != null) {
            if (c1.compareTo(c2) >= 0) {
                return c1;
            }
            else {
                return c2;
            }
        }
        else {
            return null;
        }
    }

    private boolean addToFilter(BloomFilter filter, Object value)
    {
        if (value instanceof Long) {
            filter.add(((Long) value).longValue());
        }
        else if (value instanceof Double) {
            filter.add((Double) value);
        }
        else if (value instanceof Integer) {
            filter.add((Integer) value);
        }
        else if (value instanceof Float) {
            filter.add((Float) value);
        }
        else if (value instanceof Slice) {
            filter.add((Slice) value);
        }
        else if (value instanceof byte[]) {
            filter.add((byte[]) value);
        }
        else if (value instanceof String) {
            filter.add(((String) value).getBytes());
        }
        else {
            return false;
        }

        return true;
    }

    @VisibleForTesting
    public boolean testFilter(BloomFilter filter, Object value)
    {
        if (value instanceof Long) {
            return filter.test(((Long) value).longValue());
        }
        else if (value instanceof Double) {
            return filter.test((Double) value);
        }
        else if (value instanceof Integer) {
            return filter.test((Integer) value);
        }
        else if (value instanceof Float) {
            return filter.test((Float) value);
        }
        else if (value instanceof Slice) {
            return filter.test((Slice) value);
        }
        else if (value instanceof byte[]) {
            return filter.test((byte[]) value);
        }
        else if (value instanceof String) {
            return filter.test(((String) value).getBytes());
        }
        else {
            return true;
        }
    }

    boolean canAdd()
    {
        return processingState.get() == ProcessingState.NOT_STARTED && byteSize < maxLogicalPartBytes;
    }

    AtomicReference<ProcessingState> getProcessingState()
    {
        return processingState;
    }

    private static Object getNativeValue(Object object)
    {
        return object instanceof Slice ? ((Slice) object).toStringUtf8() : object;
    }

    private static Object getNativeValue(Type type, Block block, int position)
    {
        Object obj = TypeUtils.readNativeValue(type, block, position);
        Class<?> javaType = type.getJavaType();

        if (obj != null && javaType == Slice.class) {
            obj = ((Slice) obj).toStringUtf8();
        }

        return obj;
    }

    private void readObject(ObjectInputStream in)
            throws ClassNotFoundException, IOException
    {
        in.defaultReadObject();
        int typeSize = in.readInt();
        this.typeSignatures = new ArrayList<>(typeSize);
        for (int i = 0; i < typeSize; i++) {
            typeSignatures.add(TYPE_SIGNATURE_JSON_CODEC.fromJson(in.readUTF()));
        }
    }

    private void writeObject(ObjectOutputStream out)
            throws IOException
    {
        out.defaultWriteObject();
        out.writeInt(types.size());
        for (Type type : types) {
            out.writeUTF(TYPE_SIGNATURE_JSON_CODEC.toJson(type.getTypeSignature()));
        }
    }
}
