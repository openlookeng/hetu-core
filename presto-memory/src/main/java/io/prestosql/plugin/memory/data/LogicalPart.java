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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.prestosql.plugin.memory.ColumnInfo;
import io.prestosql.plugin.memory.MemoryColumnHandle;
import io.prestosql.plugin.memory.SortingColumn;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeUtils;
import io.prestosql.spi.util.BloomFilter;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class LogicalPart
{
    private static final Logger LOG = Logger.get(LogicalPart.class);

    enum ProcessingState
    {
        NOT_STARTED, IN_PROGRESS, COMPLETE
    }

    private final AtomicReference<ProcessingState> processingState = new AtomicReference<>(ProcessingState.NOT_STARTED);

    private long rows;
    private long byteSize;
    private List<Page> pages = new ArrayList<>();

    private final List<Type> types;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private final List<String> indexColumns;
    private final List<Integer> indexChannels;
    private final PageSorter pageSorter;
    private final long maxLogicalPartBytes;

    // indexes
    private final TreeMap<Object, List<Page>> indexedPagesMap = new TreeMap<>();
    private final Map<Integer, BloomFilter> indexChannelFilters = new HashMap<>();
    private final Map<Integer, Map.Entry<Comparable, Comparable>> columnMinMax = new HashMap<>();

    public LogicalPart(List<ColumnInfo> columns, List<SortingColumn> sortedBy, List<String> indexColumns, PageSorter pageSorter, long maxLogicalPartBytes, TypeManager typeManager)
    {
        requireNonNull(columns, "columns is null");
        requireNonNull(sortedBy, "sortedBy is null");
        this.indexColumns = requireNonNull(indexColumns, "indexColumns is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.maxLogicalPartBytes = requireNonNull(maxLogicalPartBytes, "maxLogicalPartBytes is null");

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
            return pages;
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

        if (bloomIdxChannelsToDomainMap.isEmpty() && sparseIdxChannelsToDomainMap.isEmpty()) {
            return pages;
        }

        // check minmax filter
        for (Map.Entry<Integer, Domain> e : allIdxChannelsToDomainMap.entrySet()) {
            Domain columnPredicate = e.getValue();
            int expressionColumnIndex = e.getKey();
            if (columnMinMax.containsKey(expressionColumnIndex)) {
                List<Range> ranges = ((SortedRangeSet) (columnPredicate.getValues())).getOrderedRanges();

                // minmax currently only supports equality e.g. c=1
                if (ranges.size() != 1 || !ranges.get(0).isSingleValue()) {
                    continue;
                }

                Object lookupValue = getNativeValue(ranges.get(0).getSingleValue());
                if (lookupValue instanceof Comparable) {
                    Comparable comparableLookupValue = (Comparable) lookupValue;
                    Map.Entry<Comparable, Comparable> columMinMax = columnMinMax.get(e.getKey());
                    Comparable min = columMinMax.getKey();
                    Comparable max = columMinMax.getValue();

                    if (comparableLookupValue.compareTo(min) < 0 || comparableLookupValue.compareTo(max) > 0) {
                        // lookup value is outside minmax range, skip logicalpart
                        return Collections.emptyList();
                    }
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

            // Only single equality expressions are currently supported
            if (ranges.size() != 1 || !ranges.get(0).isSingleValue()) {
                continue;
            }

            // assume pages were sorted as follows:
            // [a->[{a,a}, {a,a}, {a,b}], b->[{b,b}, {b,b}, {b}]
            // i.e. a has three pages, b has 3 pages
            // and the expression is column=b
            // first lookup b for an exact match, which will return the second page
            // however, there is also a "b" in the last page of "a" list
            // this must be returned also
            Object lookupValue = getNativeValue(ranges.get(0).getSingleValue());
            List<Page> result = indexedPagesMap.getOrDefault(lookupValue, new ArrayList<>());

            Map.Entry<Object, List<Page>> lowerEntry = indexedPagesMap.lowerEntry(lookupValue);

            if (lowerEntry == null) {
                return result;
            }

            // determine if the last page of the lower entry should be included
            List<Page> lowerEntryPages = lowerEntry.getValue();
            Page lastPage = lowerEntryPages.get(lowerEntryPages.size() - 1);
            Object lastPageLastValue = getNativeValue(types.get(sortChannels.get(0)), lastPage.getBlock(sortChannels.get(0)), lastPage.getPositionCount() - 1);

            if (lastPageLastValue instanceof Comparable && lookupValue instanceof Comparable) {
                Comparable comparableLastValue = (Comparable) lastPageLastValue;
                Comparable comparableLookupValue = (Comparable) lookupValue;

                // if last page's last value is equal to or greater than lookup value, the last page must be included
                if (comparableLastValue.compareTo(comparableLookupValue) >= 0) {
                    result.add(lowerEntryPages.get(lowerEntryPages.size() - 1));
                }
            }
            else {
                // if unable to do a comparison, return last page anyway bc it may contain the lookup value
                result.add(lowerEntryPages.get(lowerEntryPages.size() - 1));
            }

            return result;
        }

        return pages;
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
                    pageSorter);
            pages.stream().forEach(sortBuffer::add);
            List<Page> sortedPages = new ArrayList<>();
            sortBuffer.flushTo(sortedPages::add);

            // create index
            int newRowCount = 0;
            long newByteSize = 0;
            for (Page page : sortedPages) {
                newByteSize += page.getRetainedSizeInBytes();
                newRowCount += page.getPositionCount();
                Object value = getNativeValue(types.get(sortChannels.get(0)), page.getBlock(sortChannels.get(0)), 0);
                if (value != null) {
                    indexedPagesMap.computeIfAbsent(value, e -> new LinkedList<>()).add(page);
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

        this.processingState.set(ProcessingState.COMPLETE);
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

    private boolean testFilter(BloomFilter filter, Object value)
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
}
