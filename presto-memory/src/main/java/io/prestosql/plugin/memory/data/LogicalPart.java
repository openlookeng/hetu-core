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
package io.prestosql.plugin.memory.data;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.BaseEncoding;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.PagesSerdeUtil;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
    enum LogicalPartState
    {
        ACCEPTING_PAGES, FINISHED_ADDING, PROCESSING, COMPLETED
    }

    private static final long serialVersionUID = -8601397465573888504L;
    private static final Logger LOG = Logger.get(LogicalPart.class);
    private static final JsonCodec<TypeSignature> TYPE_SIGNATURE_JSON_CODEC = JsonCodec.jsonCodec(TypeSignature.class);
    private static final String TABLE_DATA_FOLDER = "data";

    private long rows;
    private long byteSize;

    private final AtomicReference<LogicalPartState> processingState = new AtomicReference<>(LogicalPartState.ACCEPTING_PAGES);
    private final List<SortOrder> sortOrders;
    private final List<Integer> sortChannels;
    private final Set<Integer> indexChannels;
    private final long maxLogicalPartBytes;
    private final int maxPageSizeBytes;
    private final int logicalPartNum;
    private final boolean compressionEnabled;

    // indexes
    /*
    Sparse index is constructed as follows, assuming pages are sorted.
    Pages:
    0 {a,a,a,a,a}
    1 {a,a,a,a,b}
    2 {b,b,b,b,c}
    3 {f,f,f,f,y}

    key -> value {[page idxs], last page's last value i.e. max range}
    a -> {[0,1], b}
    b -> {[2], c}
    f -> {[3], y}

    lookup cases:
    column=a
    return page 0 and 1 (page 1 contains b but this will be filtered out later using filter operator)

    column=b
    return page 2 and page 1 (bc page 1 ends in b, which is >= to lookup value)

    column=c
    return page 2 (no exact match but check lower entry, page 2 ends in c, which is >= to lookup value)

    column=d
    return no pages (no exact match, check lower entry, but page 2 ends in c, which is not >= to lookup value)

    There are additional cases when operator is not equality and is >, >=, <, <=, BETWEEN, IN.
    Similar logic is applied in those cases.
     */
    private final TreeMap<Comparable, SparseValue> sparseIdx = new TreeMap<>();
    private final Map<Integer, BloomFilter> bloomIdx = new HashMap<>();
    private final Map<Integer, Map.Entry<Comparable, Comparable>> minMaxIdx = new HashMap<>();

    private transient Path tableDataRoot;
    private transient PagesSerde pagesSerde;
    private transient PageSorter pageSorter;
    private transient List<TypeSignature> typeSignatures;
    private transient List<Type> types;
    // Using majority of memory and disk space. Serialized and deserialized separately. Only loaded when used.
    private transient List<Page> pages;

    public LogicalPart(
            List<MemoryColumnHandle> columns,
            List<SortingColumn> sortedBy,
            List<String> indexColumns,
            Path tableDataRoot,
            PageSorter pageSorter,
            long maxLogicalPartBytes,
            int maxPageSizeBytes,
            TypeManager typeManager,
            PagesSerde pagesSerde,
            int logicalPartNum,
            boolean compressionEnabled)
    {
        this.tableDataRoot = tableDataRoot;
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
        for (MemoryColumnHandle column : columns) {
            types.add(column.getType(typeManager));
        }

        indexChannels = new HashSet<>();
        sortChannels = new ArrayList<>();
        sortOrders = new ArrayList<>();

        for (SortingColumn sortingColumn : sortedBy) {
            String sortColumnName = sortingColumn.getColumnName();
            for (MemoryColumnHandle column : columns) {
                if (column.getColumnName().equalsIgnoreCase(sortColumnName)) {
                    sortChannels.add(column.getColumnIndex());
                    sortOrders.add(sortingColumn.getOrder());
                    indexChannels.add(column.getColumnIndex());
                    break;
                }
            }
        }

        for (String indexColumn : indexColumns) {
            for (MemoryColumnHandle column : columns) {
                if (column.getColumnName().equalsIgnoreCase(indexColumn)) {
                    indexChannels.add(column.getColumnIndex());
                }
            }
        }

        this.processingState.set(LogicalPartState.ACCEPTING_PAGES);
    }

    long getByteSize()
    {
        return byteSize;
    }

    public int getLogicalPartNum()
    {
        return logicalPartNum;
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
        byteSize += page.getSizeInBytes();
    }

    boolean pageInMemory()
    {
        return pages != null;
    }

    void unloadPages()
    {
        pages = null;
    }

    void finishAdding()
    {
        processingState.set(LogicalPartState.FINISHED_ADDING);
    }

    List<Page> getPages()
    {
        if (!pageInMemory()) {
            try {
                readPages();
            }
            catch (Exception e) {
                LOG.error("Failed to load pages from " + getPageFileName(), e);
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
        if (processingState.get() != LogicalPartState.COMPLETED) {
            return getPages();
        }

        // determine which columns in the predicate can utilize indexes
        Map<Integer, List<Range>> minmaxChannelsToRangesMap = new HashMap<>();
        Map<Integer, List<Range>> bloomChannelsToRangesMap = new HashMap<>();
        Map<Integer, List<Range>> sparseChannelsToRangesMap = new HashMap<>();
        for (Map.Entry<ColumnHandle, Domain> e : predicate.getDomains().orElse(Collections.emptyMap()).entrySet()) {
            int expressionColumnIndex = ((MemoryColumnHandle) e.getKey()).getColumnIndex();
            List<Range> ranges = ((SortedRangeSet) e.getValue().getValues()).getOrderedRanges();

            // e.g. column=null
            if (ranges.isEmpty()) {
                continue;
            }

            if (minMaxIdx.containsKey(expressionColumnIndex)) {
                minmaxChannelsToRangesMap.put(expressionColumnIndex, ranges);
            }

            if (bloomIdx.containsKey(expressionColumnIndex)) {
                bloomChannelsToRangesMap.put(expressionColumnIndex, ranges);
            }

            if (sortChannels.contains(expressionColumnIndex)) {
                sparseChannelsToRangesMap.put(expressionColumnIndex, ranges);
            }
        }

        // no index to help with filtering
        if (minmaxChannelsToRangesMap.isEmpty() && bloomChannelsToRangesMap.isEmpty() && sparseChannelsToRangesMap.isEmpty()) {
            return getPages();
        }

        return getPages(minmaxChannelsToRangesMap, bloomChannelsToRangesMap, sparseChannelsToRangesMap);
    }

    /**
     * Applies the provided indexes. This method assumes that the indexes exist for the provided mappings,
     * i.e. map.contains(channelNum) should've been done earlier or an NPE may occur.
     * An additional check is not done to reduce unnecessary lookups.
     * @param minmaxChannelsToRangesMap
     * @param bloomChannelsToRangesMap
     * @param sparseChannelsToRangesMap
     * @return
     */
    List<Page> getPages(
            Map<Integer, List<Range>> minmaxChannelsToRangesMap,
            Map<Integer, List<Range>> bloomChannelsToRangesMap,
            Map<Integer, List<Range>> sparseChannelsToRangesMap)
    {
        // minmax index
        // if any column has no range match, the whole logipart can be filtered since it is assumed all column
        // predicates are AND'd together
        for (Map.Entry<Integer, List<Range>> e : minmaxChannelsToRangesMap.entrySet()) {
            int expressionColumnIndex = e.getKey();
            List<Range> ranges = e.getValue();
            // only filter using minmax if all ranges do not match
            int noMatches = 0;
            for (Range range : ranges) {
                if (range.isSingleValue()) {
                    Object lookupValue = getNativeValue(range.getSingleValue());
                    if (lookupValue instanceof Comparable) {
                        Comparable comparableLookupValue = (Comparable) lookupValue;
                        // assumes minMaxIdx map will contain the entry since the check should've been done earlier
                        Map.Entry<Comparable, Comparable> columnMinMaxEntry = minMaxIdx.get(e.getKey());
                        Comparable min = columnMinMaxEntry.getKey();
                        Comparable max = columnMinMaxEntry.getValue();

                        if (comparableLookupValue.compareTo(min) < 0 || comparableLookupValue.compareTo(max) > 0) {
                            // lookup value is outside minmax range, skip logicalpart
                            noMatches++;
                        }
                    }
                    else {
                        // the lookup value isn't comparable, we can't do filtering, e.g. if it's null
                        LOG.warn("Lookup value is not Comparable. MinMax index could not be used.");
                        return getPages();
                    }
                }
                else {
                    // <, <=, >=, >, BETWEEN
                    boolean highBoundless = range.getHigh().isUpperUnbounded();
                    boolean lowBoundless = range.getLow().isLowerUnbounded();
                    Map.Entry<Comparable, Comparable> columnMinMaxEntry = minMaxIdx.get(e.getKey());
                    Comparable min = columnMinMaxEntry.getKey();
                    Comparable max = columnMinMaxEntry.getValue();
                    if (highBoundless && !lowBoundless) {
                        // >= or >
                        Object lowLookupValue = getNativeValue(range.getLow().getValue());
                        if (lowLookupValue instanceof Comparable) {
                            Comparable lowComparableLookupValue = (Comparable) lowLookupValue;
                            boolean inclusive = range.getLow().getBound().equals(Marker.Bound.EXACTLY);
                            if (inclusive) {
                                if (lowComparableLookupValue.compareTo(max) > 0) {
                                    // lookup value is outside minmax range, skip logicalpart
                                    noMatches++;
                                }
                            }
                            else {
                                if (lowComparableLookupValue.compareTo(max) >= 0) {
                                    // lookup value is outside minmax range, skip logicalpart
                                    noMatches++;
                                }
                            }
                        }
                        else {
                            // the lookup value isn't comparable, we can't do filtering, e.g. if it's null
                            LOG.warn("Lookup value is not Comparable. MinMax index could not be used.");
                            return getPages();
                        }
                    }
                    else if (!highBoundless && lowBoundless) {
                        // <= or <
                        Object highLookupValue = getNativeValue(range.getHigh().getValue());
                        if (highLookupValue instanceof Comparable) {
                            Comparable highComparableLookupValue = (Comparable) highLookupValue;
                            boolean inclusive = range.getHigh().getBound().equals(Marker.Bound.EXACTLY);
                            if (inclusive) {
                                if (highComparableLookupValue.compareTo(min) < 0) {
                                    // lookup value is outside minmax range, skip logicalpart
                                    noMatches++;
                                }
                            }
                            else {
                                if (highComparableLookupValue.compareTo(min) <= 0) {
                                    // lookup value is outside minmax range, skip logicalpart
                                    noMatches++;
                                }
                            }
                        }
                        else {
                            // the lookup value isn't comparable, we can't do filtering, e.g. if it's null
                            LOG.warn("Lookup value is not Comparable. MinMax index could not be used.");
                            return getPages();
                        }
                    }
                    else if (!highBoundless && !lowBoundless) {
                        // BETWEEN
                        Object lowLookupValue = getNativeValue(range.getLow().getValue());
                        Object highLookupValue = getNativeValue(range.getHigh().getValue());
                        if (lowLookupValue instanceof Comparable && highLookupValue instanceof Comparable) {
                            Comparable lowComparableLookupValue = (Comparable) lowLookupValue;
                            Comparable highComparableLookupValue = (Comparable) highLookupValue;
                            if (lowComparableLookupValue.compareTo(max) > 0 || highComparableLookupValue.compareTo(min) < 0) {
                                // lookup value is outside minmax range, skip logicalpart
                                noMatches++;
                            }
                        }
                        else {
                            // the lookup value isn't comparable, we can't do filtering, e.g. if it's null
                            LOG.warn("Lookup value is not Comparable. MinMax index could not be used.");
                            return getPages();
                        }
                    }
                }
            }

            // if all ranges for this column had no match, filter this logipart
            if (noMatches == ranges.size()) {
                return Collections.emptyList();
            }
        }

        // bloom filter index
        // if any column has no range match, the whole logipart can be filtered since it is assumed all column
        // predicates are AND'd together
        boolean match = true;
        for (Map.Entry<Integer, List<Range>> e : bloomChannelsToRangesMap.entrySet()) {
            int expressionColumnIndex = e.getKey();
            List<Range> ranges = e.getValue();

            // only filter using bloom if all values in range do not match
            int falseCount = 0;
            for (Range range : ranges) {
                if (range.isSingleValue()) {
                    Object lookupValue = getNativeValue(range.getSingleValue());
                    // assumes bloomIdx map will contain the entry since the check should've been done earlier
                    BloomFilter filter = bloomIdx.get(expressionColumnIndex);
                    if (!testFilter(filter, lookupValue)) {
                        falseCount++;
                    }
                }
            }

            // if all ranges for this column had no match, filter this logipart
            if (falseCount == ranges.size()) {
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
        for (Map.Entry<Integer, List<Range>> e : sparseChannelsToRangesMap.entrySet()) {
            List<Range> ranges = e.getValue();

            Set<Integer> result = new HashSet<>();
            for (Range range : ranges) {
                if (range.isSingleValue()) {
                    // unique value(for example: id=1, id in (1) (IN operator has multiple singleValue ranges), bound: EXACTLY
                    Object lookupValue = getNativeValue(range.getSingleValue());
                    if (!(lookupValue instanceof Comparable)) {
                        LOG.warn("Lookup value is not Comparable. Sparse index could not be queried.");
                        return getPages();
                    }
                    if (sparseIdx.containsKey(lookupValue)) {
                        result.addAll(sparseIdx.get(lookupValue).getPageIndices());
                    }

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

                    NavigableMap<Comparable, SparseValue> navigableMap = null;
                    Comparable low = null;
                    Comparable high = null;
                    if (highBoundless && !lowBoundless) {
                        // >= or >
                        if (!(range.getLow().getValue() instanceof Comparable)) {
                            LOG.warn("Lookup value is not Comparable. Sparse index could not be queried.");
                            return getPages();
                        }
                        low = (Comparable) getNativeValue(range.getLow().getValue());
                        high = sparseIdx.lastKey();
                        if (low.compareTo(high) > 0) {
                            navigableMap = Collections.emptyNavigableMap();
                        }
                        else {
                            navigableMap = sparseIdx.subMap(low, fromInclusive, high, true);
                        }
                    }
                    else if (!highBoundless && lowBoundless) {
                        // <= or <
                        if (!(range.getHigh().getValue() instanceof Comparable)) {
                            LOG.warn("Lookup value is not Comparable. Sparse index could not be queried.");
                            return getPages();
                        }
                        low = sparseIdx.firstKey();
                        high = (Comparable) getNativeValue(range.getHigh().getValue());
                        toInclusive = range.getHigh().getBound().equals(Marker.Bound.EXACTLY);
                        if (low.compareTo(high) > 0) {
                            navigableMap = Collections.emptyNavigableMap();
                        }
                        else {
                            navigableMap = sparseIdx.subMap(low, true, high, toInclusive);
                        }
                    }
                    else if (!highBoundless && !lowBoundless) {
                        // BETWEEN, non-inclusive range < && >
                        if (!(range.getLow().getValue() instanceof Comparable || range.getHigh().getValue() instanceof Comparable)) {
                            LOG.warn("Lookup value is not Comparable. Sparse index could not be queried.");
                            return getPages();
                        }
                        low = min((Comparable) getNativeValue(range.getHigh().getValue()), (Comparable) getNativeValue(range.getLow().getValue()));
                        high = max((Comparable) getNativeValue(range.getHigh().getValue()), (Comparable) getNativeValue(range.getLow().getValue()));
                        navigableMap = sparseIdx.subMap(low, fromInclusive, high, toInclusive);
                    }
                    else {
                        return getPages();
                    }

                    for (Map.Entry<Comparable, SparseValue> entry : navigableMap.entrySet()) {
                        result.addAll(entry.getValue().getPageIndices());
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

        return getPages();
    }

    private Integer getLowerPageIndex(Comparable lowestInDom, Comparable lowBound, boolean includeLowBound, Comparable highBound, boolean includeHighBound)
    {
        Map.Entry<Comparable, SparseValue> lowerSparseEntry;
        if (lowestInDom != null) {
            lowerSparseEntry = sparseIdx.lowerEntry(lowestInDom);
        }
        else {
            lowerSparseEntry = sparseIdx.floorEntry(lowBound);
        }

        if (lowerSparseEntry == null) {
            return null;
        }

        /*
        This part of the code tries to include any additional "lower" pages that may contain our value(s)
        Consider the following example (only showing the sorted column's values):
        pages = pg1[a,a,b] pg2[b,b,b] pg3[c,d,null] pg4[null,null,null]

        sparseIdx =
        a -> {pgs=[1], last=b}
        b -> {pgs=[2], last=b}
        c -> {pgs=[3], last=null}

        if user query is col=d, the sparseIdx returns no match, but we must also look floor entry of d,
        i.e. entry c -> {pgs=[3], last=null}

        in entry c the last value is null, so instead of looking through all the values in the page
        we just return lastPageIdx (the else part below)

        Consider another example:
        pages = 1[a,a,b] 2[b,b,b] 3[c,d,e] 4[null,null,null]

        sparseIdx =
        a -> {pgs=[1], last=b}
        b -> {pgs=[2], last=b}
        c -> {pgs=[3], last=e}

        In this case last value is e, so we compare with d and since value d could be in pg3 somewhere, we return lastPageIdx
        */

        List<Integer> lowerPages = lowerSparseEntry.getValue().getPageIndices();
        Integer lastPageIdx = lowerPages.get(lowerPages.size() - 1);
        Comparable lastPageLastValue = lowerSparseEntry.getValue().getLast();
        if (lastPageLastValue != null) {
            int comp = (lastPageLastValue).compareTo(lowBound);
            if (comp > 0 || (comp == 0 && includeLowBound)) {
                return lastPageIdx;
            }
        }
        else {
            // if lastPageLastValue is null, null is larger than any value
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
        switch (processingState.get()) {
            case ACCEPTING_PAGES:
            case PROCESSING:
            case COMPLETED:
                return;
        }

        processingState.set(LogicalPartState.PROCESSING);

        // sort and create sparse index
        if (!sortChannels.isEmpty()) {
            SortBuffer sortBuffer = new SortBuffer(
                    new DataSize(maxLogicalPartBytes, DataSize.Unit.BYTE),
                    types,
                    sortChannels,
                    sortOrders,
                    pageSorter,
                    maxPageSizeBytes);
            pages.forEach(sortBuffer::add);
            List<Page> sortedPages = new ArrayList<>();
            sortBuffer.flushTo(sortedPages::add);

            // create index
            int newRowCount = 0;
            long newByteSize = 0;
            for (int i = 0; i < sortedPages.size(); i++) {
                Page page = sortedPages.get(i);
                newByteSize += page.getSizeInBytes();
                newRowCount += page.getPositionCount();
                Object value = getNativeValue(types.get(sortChannels.get(0)), page.getBlock(sortChannels.get(0)), 0);
                if (value != null) {
                    if (!(value instanceof Comparable)) {
                        throw new RuntimeException(String.format("Unable to create sparse index for channel %d, type is not Comparable.", sortChannels.get(0)));
                    }
                    sparseIdx.computeIfAbsent((Comparable) value, e -> new SparseValue(new ArrayList<>())).getPageIndices().add(i);
                }
            }
            for (SparseValue sparseValue : sparseIdx.values()) {
                int lastPageIndex = sparseValue.getPageIndices().get(sparseValue.getPageIndices().size() - 1);
                Page lastPage = sortedPages.get(lastPageIndex);
                sparseValue.setLast((Comparable) getNativeValue(types.get(sortChannels.get(0)), lastPage.getBlock(sortChannels.get(0)), lastPage.getPositionCount() - 1));
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
                minMaxIdx.put(sortChannels.get(0), new AbstractMap.SimpleEntry<>((Comparable) minValue, (Comparable) maxValue));
            }

            this.byteSize = newByteSize;
            this.pages.clear(); // help triggering GC of old pages
            this.pages = sortedPages;
        }

        // create bloom index on index columns
        for (Integer indexChannel : indexChannels) {
            Set<Object> values = new HashSet<>();
            for (Page page : getPages()) {
                for (int i = 0; i < page.getPositionCount(); i++) {
                    Object value = getNativeValue(types.get(indexChannel), page.getBlock(indexChannel), i);
                    if (value != null) {
                        values.add(value);
                    }
                }
            }

            BloomFilter filter = new BloomFilter(values.size(), 0.05);
            boolean unsupportedValue = false;
            // if the column is being sorted on, we already have min-max values by looking at the
            // first and last value of the pages, so we can save some computation by skipping this step
            // however, if the column is not being sorted on, the min-max values will need to be
            // determined by doing comparisons
            boolean createMinMax = !minMaxIdx.containsKey(indexChannel);
            Comparable min = null;
            Comparable max = null;
            for (Object value : values) {
                if (createMinMax && value instanceof Comparable) {
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
                minMaxIdx.put(indexChannel, new AbstractMap.SimpleEntry<>(min, max));
            }

            if (unsupportedValue) {
                continue;
            }

            bloomIdx.put(indexChannel, filter);
        }

        try {
            writePages();
        }
        catch (Exception e) {
            LOG.error("Error spilling LogicalPart " + getPageFileName() + " to disk. Restoring will be unavailable.", e);
        }
        this.processingState.set(LogicalPartState.COMPLETED);
    }

    private String getPageFileName()
    {
        return "logicalPartNumber" + logicalPartNum;
    }

    /**
     * Deserialize pages from disk
     */
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

    /**
     * Serialize pages to disk
     * @throws IOException
     */
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
        return processingState.get() == LogicalPartState.ACCEPTING_PAGES && byteSize < maxLogicalPartBytes;
    }

    AtomicReference<LogicalPartState> getProcessingState()
    {
        return processingState;
    }

    static Map<String, Page> partitionPage(Page page, List<String> partitionedBy, List<MemoryColumnHandle> columns, TypeManager typeManager)
    {
        // derive the channel numbers that corresponds to the partitionedBy list
        List<MemoryColumnHandle> partitionChannels = new ArrayList<>(partitionedBy.size());
        for (String name : partitionedBy) {
            for (MemoryColumnHandle handle : columns) {
                if (handle.getColumnName().equals(name)) {
                    partitionChannels.add(handle);
                }
            }
        }

        // build the partitions
        Map<String, Page> partitions = new HashMap<>();

        MemoryColumnHandle partitionColumnHandle = partitionChannels.get(0);
        Block block = page.getBlock(partitionColumnHandle.getColumnIndex());
        Type type = partitionColumnHandle.getType(typeManager);
        Map<Object, ArrayList<Integer>> uniqueValues = new HashMap<>();
        for (int i = 0; i < page.getPositionCount(); i++) {
            Object value = getNativeValue(type, block, i);
            uniqueValues.putIfAbsent(value, new ArrayList<>());
            uniqueValues.get(value).add(i);
        }

        for (Map.Entry<Object, ArrayList<Integer>> valueAndPosition : uniqueValues.entrySet()) {
            int[] retainedPositions = valueAndPosition.getValue().stream().mapToInt(i -> i).toArray();
            Object valueKey = valueAndPosition.getKey();
            Page subPage = page.getPositions(retainedPositions, 0, retainedPositions.length);
            partitions.put(valueKey.toString(), subPage);
        }
        return partitions;
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
            Slice slice = (Slice) obj;
            TypeSignature typeSig = type.getTypeSignature();
            if (typeSig.getBase().equals("uuid")) {
                obj = BaseEncoding.base16().encode(slice.getBytes());
            }
            else {
                obj = slice.toStringUtf8();
            }
        }
        return obj;
    }

    // supported partition value types: BOOLEAN, All INT Types, CHAR, VARCHAR, DOUBLE, REAL, DECIMAL, DATE, TIME, UUID
    public static Object deserializeTypedValueFromString(Type type, String serialized)
    {
        if (serialized == null) {
            return null;
        }
        else if (type.getJavaType() == boolean.class) {
            return Boolean.parseBoolean(serialized);
        }
        else if (type.getJavaType() == long.class) {
            return Long.parseLong(serialized);
        }
        else if (type.getJavaType() == double.class) {
            return Double.parseDouble(serialized);
        }
        else if (type.getJavaType() == Slice.class) {
            TypeSignature typeSig = type.getTypeSignature();
            if (typeSig.getBase().equals("uuid")) {
                return Slices.wrappedBuffer(BaseEncoding.base16().decode(serialized));
            }
            else {
                return Slices.utf8Slice(serialized);
            }
        }
        else {
            throw new IllegalStateException("Unable to deserialize value");
        }
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

    static class SparseValue
            implements Serializable
    {
        List<Integer> pageIndices;
        Comparable last;

        public SparseValue(List<Integer> pageIndices)
        {
            this.pageIndices = pageIndices;
            this.last = null;
        }

        public SparseValue(List<Integer> pageIndices, Comparable last)
        {
            this.pageIndices = pageIndices;
            this.last = last;
        }

        public List<Integer> getPageIndices()
        {
            return pageIndices;
        }

        public Comparable getLast()
        {
            return last;
        }

        public void setLast(Comparable last)
        {
            this.last = last;
        }
    }
}
