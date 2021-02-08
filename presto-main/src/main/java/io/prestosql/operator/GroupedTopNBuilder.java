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
package io.prestosql.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.operator.window.RankingFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class finds the top N rows defined by {@param comparator} for each group specified by {@param groupByHash}.
 */
public class GroupedTopNBuilder
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(GroupedTopNBuilder.class).instanceSize();
    // compact a page when 50% of its positions are unreferenced
    private static final int COMPACT_THRESHOLD = 2;

    private final List<Type> sourceTypes;
    private final int topN;

    private final Optional<RankingFunction> rankingFunction;
    private final boolean produceRankingNumber;
    private final GroupByHash groupByHash;

    // a map of heaps, each of which records the top N rows
    private final ObjectBigArray<RowHeap> groupedRows = new ObjectBigArray<>();
    // a list of input pages, each of which has information of which row in which heap references which position
    private final ObjectBigArray<PageReference> pageReferences = new ObjectBigArray<>();
    // for heap element comparison
    private final Comparator<Row> comparator;
    // when there is no row referenced in a page, it will be removed instead of compacted; use a list to record those empty slots to reuse them
    private final IntFIFOQueue emptyPageReferenceSlots;

    // keeps track sizes of input pages and heaps
    private long memorySizeInBytes;
    private int currentPageCount;

    public GroupedTopNBuilder(
            List<Type> sourceTypes,
            PageWithPositionComparator comparator,
            int topN,
            boolean produceRankingNumber,
            Optional<RankingFunction> rankingFunction,
            GroupByHash groupByHash)
    {
        this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
        checkArgument(topN > 0, "topN must be > 0");
        this.topN = topN;
        this.groupByHash = requireNonNull(groupByHash, "groupByHash is not null");
        this.rankingFunction = requireNonNull(rankingFunction, "rankingFunction is null");
        this.produceRankingNumber = produceRankingNumber;

        requireNonNull(comparator, "comparator is null");
        this.comparator = (left, right) -> comparator.compareTo(
                pageReferences.get(left.getPageId()).getPage(),
                left.getPosition(),
                pageReferences.get(right.getPageId()).getPage(),
                right.getPosition());
        this.emptyPageReferenceSlots = new IntFIFOQueue();
    }

    public Work<?> processPage(Page page)
    {
        return new TransformWork<>(
                groupByHash.getGroupIds(page),
                groupIds -> {
                    processPage(page, groupIds);
                    return null;
                });
    }

    public Iterator<Page> buildResult()
    {
        return new ResultIterator();
    }

    public long getEstimatedSizeInBytes()
    {
        return INSTANCE_SIZE +
                memorySizeInBytes +
                groupByHash.getEstimatedSize() +
                groupedRows.sizeOf() +
                pageReferences.sizeOf() +
                emptyPageReferenceSlots.getEstimatedSizeInBytes();
    }

    @VisibleForTesting
    List<Page> getBufferedPages()
    {
        return IntStream.range(0, currentPageCount)
                .filter(i -> pageReferences.get(i) != null)
                .mapToObj(i -> pageReferences.get(i).getPage())
                .collect(toImmutableList());
    }

    private void processPage(Page newPage, GroupByIdBlock groupIds)
    {
        checkArgument(newPage != null);
        checkArgument(groupIds != null);

        // save the new page
        PageReference newPageReference = new PageReference(newPage);
        int newPageId;
        if (emptyPageReferenceSlots.isEmpty()) {
            // all the previous slots are full; create a new one
            pageReferences.ensureCapacity(currentPageCount + 1);
            newPageId = currentPageCount;
            currentPageCount++;
        }
        else {
            // reuse a previously removed page's slot
            newPageId = emptyPageReferenceSlots.dequeueInt();
        }
        verify(pageReferences.get(newPageId) == null, "should not overwrite a non-empty slot");
        pageReferences.set(newPageId, newPageReference);

        // update the affected heaps and record candidate pages that need compaction
        IntSet pagesToCompact = new IntOpenHashSet();
        for (int position = 0; position < newPage.getPositionCount(); position++) {
            long groupId = groupIds.getGroupId(position);
            groupedRows.ensureCapacity(groupId + 1);

            RowHeap rowheap = groupedRows.get(groupId);
            if (rowheap == null) {
                // a new group
                rowheap = getRowHeap(rankingFunction);
                groupedRows.set(groupId, rowheap);
            }
            else {
                // update an existing group;
                // remove the memory usage for this group for now; add it back after update
                memorySizeInBytes -= rowheap.getEstimatedSizeInBytes();
            }

            if (rowheap.isNotFull()) {
                // still have space for the current group
                Row row = new Row(newPageId, position);
                rowheap.enqueue(row);
                newPageReference.reference(row);
            }
            else {
                // may compare with the topN-th element with in the heap to decide if update is necessary
                Row newRow = new Row(newPageId, position);
                int compareValue = comparator.compare(newRow, rowheap.first());
                if (compareValue <= 0) {
                    //update reference and the heap
                    newPageReference.reference(newRow);
                    List<Row> rowList = rowheap.tryRemoveFirst(compareValue == 0, newRow);
                    rowheap.enqueue(newRow);

                    if (rowList != null) {
                        for (Row previousRow : rowList) {
                            PageReference previousPageReference = pageReferences.get(previousRow.getPageId());
                            previousPageReference.dereference(previousRow.getPosition());
                            // compact a page if it is not the current input page and the reference count is below the threshold
                            if (previousPageReference.getPage() != newPage &&
                                    previousPageReference.getUsedPositionCount() * COMPACT_THRESHOLD < previousPageReference.getPage().getPositionCount()) {
                                pagesToCompact.add(previousRow.getPageId());
                            }
                        }
                    }
                }
            }
            memorySizeInBytes += rowheap.getEstimatedSizeInBytes();
        }

        // unreference new page if it was not used
        if (newPageReference.getUsedPositionCount() == 0) {
            pageReferences.set(newPageId, null);
        }
        else {
            // assure new page is loaded
            newPageReference.loadPage();
            memorySizeInBytes += newPageReference.getEstimatedSizeInBytes();

            // may compact the new page as well
            if (newPageReference.getUsedPositionCount() * COMPACT_THRESHOLD < newPage.getPositionCount()) {
                verify(!pagesToCompact.contains(newPageId));
                pagesToCompact.add(newPageId);
            }
        }

        // compact pages
        IntIterator iterator = pagesToCompact.iterator();
        while (iterator.hasNext()) {
            int pageId = iterator.nextInt();
            PageReference pageReference = pageReferences.get(pageId);
            if (pageReference.getUsedPositionCount() == 0) {
                pageReferences.set(pageId, null);
                emptyPageReferenceSlots.enqueue(pageId);
                memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
            }
            else {
                memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
                pageReference.compact();
                memorySizeInBytes += pageReference.getEstimatedSizeInBytes();
            }
        }
    }

    public RowHeap getRowHeap(Optional<RankingFunction> rankingFunction)
    {
        if (rankingFunction.isPresent()) {
            switch (rankingFunction.get()) {
                case RANK:
                    return new RankRowHeap(comparator, topN);
                case DENSE_RANK:
                    return new DenseRankRowHeap(comparator, topN);
                case ROW_NUMBER:
                    return new RowNumberRowHeap(comparator, topN);
                default:
                    throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, format("Not Support TopN Ranking function :%s", rankingFunction.get()));
            }
        }
        else {
            return new RowNumberRowHeap(comparator, topN);
        }
    }

    /**
     * The class is a pointer to a row in a page.
     * The actual position in the page is mutable because as pages are compacted, the position will change.
     */
    private static class Row
    {
        private final int pageId;
        private int position;

        private Row(int pageId, int position)
        {
            this.pageId = pageId;
            reset(position);
        }

        public void reset(int position)
        {
            this.position = position;
        }

        public int getPageId()
        {
            return pageId;
        }

        public int getPosition()
        {
            return position;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("pageId", pageId)
                    .add("position", position)
                    .toString();
        }
    }

    public static class PageReference
    {
        private static final long INSTANCE_SIZE = ClassLayout.parseClass(PageReference.class).instanceSize();

        private Page page;
        private Row[] reference;

        private int usedPositionCount;

        public PageReference(Page page)
        {
            this.page = requireNonNull(page, "page is null");
            this.reference = new Row[page.getPositionCount()];
        }

        public void reference(Row row)
        {
            int position = row.getPosition();
            reference[position] = row;
            usedPositionCount++;
        }

        public void dereference(int position)
        {
            checkArgument(reference[position] != null && usedPositionCount > 0);
            reference[position] = null;
            usedPositionCount--;
        }

        public int getUsedPositionCount()
        {
            return usedPositionCount;
        }

        public void compact()
        {
            checkState(usedPositionCount > 0);

            if (usedPositionCount == page.getPositionCount()) {
                return;
            }

            // re-assign reference
            Row[] newReference = new Row[usedPositionCount];
            int[] positions = new int[usedPositionCount];
            int index = 0;
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (reference[i] != null) {
                    newReference[index] = reference[i];
                    positions[index] = i;
                    index++;
                }
            }
            verify(index == usedPositionCount);

            // compact page
            Block[] blocks = new Block[page.getChannelCount()];
            for (int i = 0; i < page.getChannelCount(); i++) {
                Block block = page.getBlock(i);
                blocks[i] = block.copyPositions(positions, 0, usedPositionCount);
            }

            // update all the elements in the heaps that reference the current page
            for (int i = 0; i < usedPositionCount; i++) {
                // this does not change the elements in the heap;
                // it only updates the value of the elements; while keeping the same order
                newReference[i].reset(i);
            }
            page = new Page(usedPositionCount, blocks);
            reference = newReference;
        }

        public void loadPage()
        {
            page = page.getLoadedPage();
        }

        public Page getPage()
        {
            return page;
        }

        public long getEstimatedSizeInBytes()
        {
            return page.getRetainedSizeInBytes() + sizeOf(reference) + INSTANCE_SIZE;
        }
    }

    // this class is for precise memory tracking
    private static class IntFIFOQueue
            extends IntArrayFIFOQueue
    {
        private static final long INSTANCE_SIZE = ClassLayout.parseClass(IntFIFOQueue.class).instanceSize();

        private long getEstimatedSizeInBytes()
        {
            return INSTANCE_SIZE + sizeOf(array);
        }
    }

    private abstract static class RowHeap
            extends ObjectHeapPriorityQueue<Row>
    {
        static final long ROW_ENTRY_SIZE = ClassLayout.parseClass(Row.class).instanceSize();

        protected int topN;

        static class RowList
        {
            List<Row> rows;
            int virtualSize;

            public RowList()
            {
                rows = new LinkedList<>();
                virtualSize = 0;
            }

            public void addRow(Row row)
            {
                rows.add(row);
                virtualSize++;
            }

            public int virtualRemoveRow()
            {
                return --virtualSize;
            }
        }

        public RowHeap(Comparator<Row> comparator, int topN)
        {
            super(1, comparator);
            this.topN = topN;
        }

        public void enqueue(Row row)
        {
            super.enqueue(row);
        }

        public Row dequeue()
        {
            return super.dequeue();
        }

        abstract List<Row> tryRemoveFirst(boolean isEqual, Row newRow);

        public boolean isNotFull()
        {
            return super.size() < topN;
        }

        public boolean isEmpty()
        {
            return super.isEmpty();
        }

        public Row first()
        {
            return super.first();
        }

        public void clear()
        {
            super.clear();
        }

        public int size()
        {
            return size;
        }

        abstract long getEstimatedSizeInBytes();
    }

    private static class RowNumberRowHeap
            extends RowHeap
    {
        static final long INSTANCE_SIZE = ClassLayout.parseClass(RowNumberRowHeap.class).instanceSize();

        private RowNumberRowHeap(Comparator<Row> comparator, int topN)
        {
            super(Ordering.from(comparator).reversed(), topN);
        }

        @Override
        public List<Row> tryRemoveFirst(boolean isEqual, Row newRow)
        {
            return ImmutableList.of(dequeue());
        }

        @Override
        public long getEstimatedSizeInBytes()
        {
            return INSTANCE_SIZE + sizeOf(heap) + size * ROW_ENTRY_SIZE;
        }
    }

    private static class DenseRankRowHeap
            extends RowHeap
    {
        static final long INSTANCE_SIZE = ClassLayout.parseClass(DenseRankRowHeap.class).instanceSize();
        private final TreeMap<Row, List<Row>> rowMaps;

        public DenseRankRowHeap(Comparator<Row> comparator, int topN)
        {
            super(Ordering.from(comparator).reversed(), topN);
            this.rowMaps = new TreeMap<>(Ordering.from(comparator).reversed());
        }

        @Override
        public void enqueue(Row row)
        {
            if (rowMaps.containsKey(row)) {
                rowMaps.get(row).add(row);
            }
            else {
                super.enqueue(row);
                List<Row> rows = new ArrayList<>();
                rows.add(row);
                rowMaps.put(row, rows);
            }
        }

        @Override
        public Row dequeue()
        {
            Row row = super.first();
            List<Row> rowList = rowMaps.get(row);
            Row dequeueRow = rowList.remove(0);

            if (rowList.size() == 0) {
                super.dequeue();
                rowMaps.remove(row);
            }
            return dequeueRow;
        }

        @Override
        public List<Row> tryRemoveFirst(boolean isEqual, Row newRow)
        {
            if (rowMaps.containsKey(newRow)) {
                return null;
            }
            else {
                Row row = super.dequeue();
                return rowMaps.remove(row);
            }
        }

        @Override
        public void clear()
        {
            super.clear();
            rowMaps.clear();
        }

        @Override
        public int size()
        {
            return rowMaps
                    .values()
                    .stream()
                    .map(List::size)
                    .reduce(0, (a, b) -> a + b);
        }

        @Override
        public long getEstimatedSizeInBytes()
        {
            // Inaccurate memory statistics, excluding TreeMap
            return INSTANCE_SIZE + sizeOf(heap) + size() * ROW_ENTRY_SIZE;
        }
    }

    private static class RankRowHeap
            extends RowHeap
    {
        static final long INSTANCE_SIZE = ClassLayout.parseClass(RankRowHeap.class).instanceSize();
        private final TreeMap<Row, RowList> rowMaps;

        public RankRowHeap(Comparator<Row> comparator, int topN)
        {
            super(Ordering.from(comparator).reversed(), topN);
            rowMaps = new TreeMap<>(Ordering.from(comparator).reversed());
        }

        @Override
        public void enqueue(Row row)
        {
            if (!rowMaps.containsKey(row)) {
                rowMaps.put(row, new RowList());
            }
            rowMaps.get(row).addRow(row);
            super.enqueue(row);
        }

        public List<Row> tryRemoveFirst(boolean isEqual, Row newRow)
        {
            if (!isEqual) {
                Row first = super.first();
                RowList rowList = rowMaps.get(first);
                if (rowList != null) {
                    if (rowList.virtualRemoveRow() == 0) {
                        RowList removedRows = rowMaps.remove(first);
                        for (int i = 0; i < removedRows.rows.size(); i++) {
                            super.dequeue();
                        }
                        return removedRows.rows;
                    }
                }
            }
            return null;
        }

        @Override
        public void clear()
        {
            super.clear();
            rowMaps.clear();
        }

        @Override
        public long getEstimatedSizeInBytes()
        {
            // Inaccurate memory statistics, excluding TreeMap
            return INSTANCE_SIZE + sizeOf(heap) + size() * ROW_ENTRY_SIZE;
        }
    }

    private interface RankingNumberBuilder
    {
        int generateRankingNumber(Row currentRow, boolean isNewGroup);
    }

    private class RowNumberBuilder
            implements RankingNumberBuilder
    {
        private int previousRowNumber;

        @Override
        public int generateRankingNumber(Row currentRow, boolean isNewGroup)
        {
            if (isNewGroup) {
                previousRowNumber = 1;
            }
            else {
                previousRowNumber += 1;
            }
            return previousRowNumber;
        }
    }

    private class RankNumberBuilder
            implements RankingNumberBuilder
    {
        private final Comparator<Row> comparator;
        private Row previousRow;
        private int previousRowNumber;
        private int currentCount;

        public RankNumberBuilder(Comparator<Row> comparator)
        {
            this.comparator = comparator;
        }

        @Override
        public int generateRankingNumber(Row row, boolean isNewGroup)
        {
            if (isNewGroup) {
                previousRowNumber = 1;
                currentCount = 1;
            }
            else {
                currentCount++;
                if (comparator.compare(row, previousRow) > 0) {
                    previousRowNumber = currentCount;
                }
            }
            previousRow = row;
            return previousRowNumber;
        }
    }

    private class DenseRankNumberBuilder
            implements RankingNumberBuilder
    {
        private final Comparator<Row> comparator;
        private Row previousRow;
        private int previousRowNumber;

        public DenseRankNumberBuilder(Comparator<Row> comparator)
        {
            this.comparator = comparator;
        }

        @Override
        public int generateRankingNumber(Row row, boolean isNewGroup)
        {
            if (isNewGroup) {
                previousRowNumber = 1;
            }
            else {
                if (comparator.compare(row, previousRow) > 0) {
                    previousRowNumber += 1;
                }
            }
            previousRow = row;
            return previousRowNumber;
        }
    }

    private class ResultIterator
            extends AbstractIterator<Page>
    {
        private final PageBuilder pageBuilder;
        // we may have 0 groups if there is no input page processed
        private final int groupCount = groupByHash.getGroupCount();

        private int currentGroupNumber;
        private long currentGroupSizeInBytes;

        // the row number of the current position in the group
        private int currentGroupPosition;
        // number of rows in the group
        private int currentGroupSize;

        private RankingNumberBuilder rankingNumberBuilder;

        private ObjectBigArray<Row> currentRows = nextGroupedRows();

        ResultIterator()
        {
            if (produceRankingNumber) {
                pageBuilder = new PageBuilder(new ImmutableList.Builder<Type>().addAll(sourceTypes).add(BIGINT).build());
                this.rankingNumberBuilder = getRankingFunctionRankingNumberBuilder(rankingFunction.get());
            }
            else {
                pageBuilder = new PageBuilder(sourceTypes);
            }
        }

        public RankingNumberBuilder getRankingFunctionRankingNumberBuilder(RankingFunction rankingFunction)
        {
            switch (rankingFunction) {
                case RANK:
                    return new RankNumberBuilder(comparator);
                case ROW_NUMBER:
                    return new RowNumberBuilder();
                case DENSE_RANK:
                    return new DenseRankNumberBuilder(comparator);
                default:
                    throw new IllegalArgumentException("Unsupported rankingFunction: " + rankingFunction);
            }
        }

        @Override
        protected Page computeNext()
        {
            pageBuilder.reset();
            while (!pageBuilder.isFull()) {
                if (currentRows == null) {
                    // no more groups
                    break;
                }
                if (currentGroupPosition == currentGroupSize) {
                    // the current group has produced all its rows
                    for (int i = 0; i < currentGroupPosition; i++) {
                        Row row = currentRows.get(i);
                        removeRow(row);
                    }
                    memorySizeInBytes -= currentGroupSizeInBytes;
                    currentGroupPosition = 0;
                    currentRows = nextGroupedRows();
                    continue;
                }

                Row row = currentRows.get(currentGroupPosition);
                if (produceRankingNumber) {
                    long rankingNumber = rankingNumberBuilder.generateRankingNumber(row, currentGroupPosition == 0);
                    if (rankingNumber > topN) {
                        currentGroupPosition++;
                        continue;
                    }
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(sourceTypes.size()), rankingNumber);
                }
                for (int i = 0; i < sourceTypes.size(); i++) {
                    sourceTypes.get(i).appendTo(pageReferences.get(row.getPageId()).getPage().getBlock(i), row.getPosition(), pageBuilder.getBlockBuilder(i));
                }

                pageBuilder.declarePosition();
                currentGroupPosition++;
            }

            if (pageBuilder.isEmpty()) {
                return endOfData();
            }
            return pageBuilder.build();
        }

        private void removeRow(Row row)
        {
            // deference the row; no need to compact the pages but remove them if completely unused
            PageReference pageReference = pageReferences.get(row.getPageId());
            pageReference.dereference(row.getPosition());
            if (pageReference.getUsedPositionCount() == 0) {
                pageReferences.set(row.getPageId(), null);
                memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
            }
        }

        private ObjectBigArray<Row> nextGroupedRows()
        {
            if (currentGroupNumber < groupCount) {
                RowHeap rows = groupedRows.get(currentGroupNumber);
                verify(rows != null && !rows.isEmpty(), "impossible to have inserted a group without a witness row");
                groupedRows.set(currentGroupNumber, null);
                currentGroupSizeInBytes = rows.getEstimatedSizeInBytes();
                currentGroupNumber++;
                currentGroupSize = rows.size();

                // sort output rows in a big array in case there are too many rows
                ObjectBigArray<Row> sortedRows = new ObjectBigArray<>();
                sortedRows.ensureCapacity(currentGroupSize);
                int index = currentGroupSize - 1;
                while (!rows.isEmpty()) {
                    sortedRows.set(index, rows.dequeue());
                    index--;
                }

                return sortedRows;
            }
            return null;
        }
    }
}
