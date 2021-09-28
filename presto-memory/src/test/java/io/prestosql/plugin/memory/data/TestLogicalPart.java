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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.RowPagesBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.util.BloomFilter;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static io.prestosql.spi.predicate.Range.equal;
import static io.prestosql.spi.predicate.Range.greaterThan;
import static io.prestosql.spi.predicate.Range.greaterThanOrEqual;
import static io.prestosql.spi.predicate.Range.lessThan;
import static io.prestosql.spi.predicate.Range.lessThanOrEqual;
import static io.prestosql.spi.predicate.Range.range;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestLogicalPart
{
    private static final long[] PAGE_VALUES = {2L, 2L, 2L, 2L, 2L, 3L, 3L, 4L, 5L, 7L, 7L, 7L};
    private static final List<Type> TYPES = ImmutableList.of(IntegerType.INTEGER);

    private List<Page> buildFourByThreePagesList(long[] pageNumbers)
    {
        return RowPagesBuilder.rowPagesBuilder(TYPES)
                .row(pageNumbers[0])
                .row(pageNumbers[1])
                .row(pageNumbers[2])
                .pageBreak()
                .row(pageNumbers[3])
                .row(pageNumbers[4])
                .row(pageNumbers[5])
                .pageBreak()
                .row(pageNumbers[6])
                .row(pageNumbers[7])
                .row(pageNumbers[8])
                .pageBreak()
                .row(pageNumbers[9])
                .row(pageNumbers[10])
                .row(pageNumbers[11])
                .build();
    }

    private void addPages(LogicalPart logicalPart, List<Page> pages)
    {
        ReflectionTestUtils.setField(logicalPart, "pages", pages);
    }

    private boolean areBlockValuesEqual(ArrayList<long[]> a, ArrayList<long[]> b)
    {
        if (a.size() != b.size()) {
            return false;
        }

        for (int i = 0; i < a.size(); i++) {
            if (a.get(i).length != b.get(i).length) {
                return false;
            }

            for (int j = 0; j < a.get(i).length; j++) {
                if (a.get(i)[j] != b.get(i)[j]) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean areListPagesEqual(List<Page> list1, List<Page> list2)
    {
        if (list1.size() != list2.size()) {
            return false;
        }

        ArrayList<long[]> possErrors1 = new ArrayList<>();
        ArrayList<long[]> possErrors2 = new ArrayList<>();

        for (int i = 0; i < list1.size(); i++) {
            int numBlockChannels = list1.get(i).getChannelCount();
            if (numBlockChannels != list2.get(i).getChannelCount()) {
                return false;
            }

            for (int j = 0; j < numBlockChannels; j++) {
                Block block1 = list1.get(i).getBlock(j);
                Block block2 = list2.get(i).getBlock(j);
                if (!block1.equals(block2)) {
                    long[] block1Val = new long[3];
                    block1Val[0] = block1.getLong(0, 0);
                    block1Val[1] = block1.getLong(1, 0);
                    block1Val[2] = block1.getLong(2, 0);

                    long[] block2Val = new long[3];
                    block2Val[0] = block2.getLong(0, 0);
                    block2Val[1] = block2.getLong(1, 0);
                    block2Val[2] = block2.getLong(2, 0);

                    possErrors1.add(block1Val);
                    possErrors2.add(block2Val);
                }
            }
        }

        Collections.sort(possErrors1, new BlockComparator());
        Collections.sort(possErrors2, new BlockComparator());

        return areBlockValuesEqual(possErrors1, possErrors2);
    }

    // min max index tests
    private LogicalPart setupMinMaxLogicalPart(long min, long max)
    {
        LogicalPart logicalPart = new LogicalPart(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Paths.get("/tmp"),
                mock(PageSorter.class),
                0,
                0,
                mock(TypeManager.class),
                mock(PagesSerde.class),
                0,
                false);
        AtomicReference<LogicalPart.LogicalPartState> processingState = new AtomicReference<>(LogicalPart.LogicalPartState.COMPLETED);
        ReflectionTestUtils.setField(logicalPart, "processingState", processingState);

        Set<Integer> indexChannels = new HashSet<>();
        indexChannels.add(0);
        ReflectionTestUtils.setField(logicalPart, "indexChannels", indexChannels);

        List<Integer> sortChannels = new ArrayList<>();
        sortChannels.add(0); // 0 is sorted
        ReflectionTestUtils.setField(logicalPart, "sortChannels", sortChannels);

        Map<Integer, Map.Entry<Comparable, Comparable>> minMaxIdx = new HashMap<>();
        Map.Entry<Comparable, Comparable> entry1 = new AbstractMap.SimpleEntry<>(min, max);
        minMaxIdx.put(0, entry1);
        ReflectionTestUtils.setField(logicalPart, "minMaxIdx", minMaxIdx);

        return logicalPart;
    }

    private List<Page> getMinMaxEqualityResult(LogicalPart logicalPart, long domainValue)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                equal(IntegerType.INTEGER, domainValue)),
                false);

        return logicalPart.getPages(ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()), Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void testGetPagesMinMaxEquality()
    {
        LogicalPart logicalPart = setupMinMaxLogicalPart(PAGE_VALUES[0], PAGE_VALUES[PAGE_VALUES.length - 1]);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(PAGE_VALUES);
        addPages(logicalPart, pages);
        long max = PAGE_VALUES[PAGE_VALUES.length - 1];
        long min = PAGE_VALUES[0];

        // 2. domain value is greater than max
        long domainValue = max + 1L;
        result = getMinMaxEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 3. domain value is lesser than min
        domainValue = min - 1L;
        result = getMinMaxEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 4. domain value is max
        domainValue = max;
        result = getMinMaxEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 5. domain value is min
        domainValue = min;
        result = getMinMaxEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 6. domain value is within bounds and is a page value, on two pages
        domainValue = 3L;
        result = getMinMaxEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 7. domain value is within bounds and is a page value, on one page, a
        domainValue = 4L;
        result = getMinMaxEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 8. domain value is within bounds and is a page value, on one page, b
        domainValue = 5L;
        result = getMinMaxEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 9. domain value is within bounds and is not a page value
        domainValue = 6L;
        result = getMinMaxEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));
    }

    private List<Page> getMinMaxInResult(LogicalPart logicalPart, Long val1, Long val2)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                equal(IntegerType.INTEGER, val1),
                equal(IntegerType.INTEGER, val2)),
                false);

        return logicalPart.getPages(ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()), Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void testGetPagesMinMaxIn()
    {
        LogicalPart logicalPart = setupMinMaxLogicalPart(PAGE_VALUES[0], PAGE_VALUES[PAGE_VALUES.length - 1]);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(PAGE_VALUES);
        addPages(logicalPart, pages);
        long max = PAGE_VALUES[PAGE_VALUES.length - 1];
        long min = PAGE_VALUES[0];

        // 2. domain value is greater than max
        long first = max + 1L;
        long second = max + 1L;
        result = getMinMaxInResult(logicalPart, first, second);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 3. domain value is lesser than min
        first = min - 1L;
        second = min - 1L;
        result = getMinMaxInResult(logicalPart, first, second);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 4. domain value is max
        first = max;
        second = max;
        result = getMinMaxInResult(logicalPart, first, second);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 5. domain value is min
        first = min;
        second = min;
        result = getMinMaxInResult(logicalPart, first, second);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 6. domain value is within bounds and is a page value, on two pages
        first = 3L;
        second = 3L;
        result = getMinMaxInResult(logicalPart, first, second);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 7. domain value is within bounds and is a page value, on one page, a
        first = 4L;
        second = 4L;
        result = getMinMaxInResult(logicalPart, first, second);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 8. domain value is within bounds and is a page value, on one page, b
        first = 5L;
        second = 5L;
        result = getMinMaxInResult(logicalPart, first, second);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 9. domain value is within bounds and is not a page value
        first = 6L;
        second = 6L;
        result = getMinMaxInResult(logicalPart, first, second);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 10. one domain value is within bounds another is not
        first = 6L;
        second = 100L;
        result = getMinMaxInResult(logicalPart, first, second);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));
    }

    private List<Page> getMinMaxGreaterResult(LogicalPart logicalPart, long domainValue)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                greaterThan(IntegerType.INTEGER, domainValue)),
                false);

        return logicalPart.getPages(ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()), Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void testGetPagesMinMaxGreater()
    {
        LogicalPart logicalPart = setupMinMaxLogicalPart(PAGE_VALUES[0], PAGE_VALUES[PAGE_VALUES.length - 1]);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(PAGE_VALUES);
        addPages(logicalPart, pages);
        long max = PAGE_VALUES[PAGE_VALUES.length - 1];
        long min = PAGE_VALUES[0];

        // 2. domain value is greater than max
        long domainValue = max + 1L;
        result = getMinMaxGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 3. domain value is lesser than min
        domainValue = min - 1L;
        result = getMinMaxGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 4. domain value is max
        domainValue = max;
        result = getMinMaxGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 5. domain value is min
        domainValue = min;
        result = getMinMaxGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 6. domain value is within bounds and is a page value, on two pages
        domainValue = 3L;
        result = getMinMaxGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 7. domain value is within bounds and is a page value, on one page, a
        domainValue = 4L;
        result = getMinMaxGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 8. domain value is within bounds and is a page value, on one page, b
        domainValue = 5L;
        result = getMinMaxGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 9. domain value is within bounds and is not a page value
        domainValue = 6L;
        result = getMinMaxGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));
    }

    private List<Page> getMinMaxGreaterEqualResult(LogicalPart logicalPart, long domainValue)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                greaterThanOrEqual(IntegerType.INTEGER, domainValue)),
                false);

        return logicalPart.getPages(ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()), Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void testGetPagesMinMaxGreaterEqual()
    {
        LogicalPart logicalPart = setupMinMaxLogicalPart(PAGE_VALUES[0], PAGE_VALUES[PAGE_VALUES.length - 1]);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(PAGE_VALUES);
        addPages(logicalPart, pages);
        long max = PAGE_VALUES[PAGE_VALUES.length - 1];
        long min = PAGE_VALUES[0];

        // 2. domain value is greater than max
        long domainValue = max + 1L;
        result = getMinMaxGreaterEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 3. domain value is lesser than min
        domainValue = min - 1L;
        result = getMinMaxGreaterEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 4. domain value is max
        domainValue = max;
        result = getMinMaxGreaterEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 5. domain value is min
        domainValue = min;
        result = getMinMaxGreaterEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 6. domain value is within bounds and is a page value, on two pages
        domainValue = 3L;
        result = getMinMaxGreaterEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 7. domain value is within bounds and is a page value, on one page, a
        domainValue = 4L;
        result = getMinMaxGreaterEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 8. domain value is within bounds and is a page value, on one page, b
        domainValue = 5L;
        result = getMinMaxGreaterEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 9. domain value is within bounds and is not a page value
        domainValue = 6L;
        result = getMinMaxGreaterEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));
    }

    private List<Page> getMinMaxLessResult(LogicalPart logicalPart, long domainValue)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                lessThan(IntegerType.INTEGER, domainValue)),
                false);

        return logicalPart.getPages(ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()), Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void testGetPagesMinMaxLess()
    {
        LogicalPart logicalPart = setupMinMaxLogicalPart(PAGE_VALUES[0], PAGE_VALUES[PAGE_VALUES.length - 1]);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(PAGE_VALUES);
        addPages(logicalPart, pages);
        long max = PAGE_VALUES[PAGE_VALUES.length - 1];
        long min = PAGE_VALUES[0];

        // 2. domain value is greater than max
        long domainValue = max + 1L;
        result = getMinMaxLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 3. domain value is lesser than min
        domainValue = min - 1L;
        result = getMinMaxLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 4. domain value is max
        domainValue = max;
        result = getMinMaxLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 5. domain value is min
        domainValue = min;
        result = getMinMaxLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 6. domain value is within bounds and is a page value, on two pages
        domainValue = 3L;
        result = getMinMaxLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 7. domain value is within bounds and is a page value, on one page, a
        domainValue = 4L;
        result = getMinMaxLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 8. domain value is within bounds and is a page value, on one page, b
        domainValue = 5L;
        result = getMinMaxLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 9. domain value is within bounds and is not a page value
        domainValue = 6L;
        result = getMinMaxLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));
    }

    private List<Page> getMinMaxLessEqualResult(LogicalPart logicalPart, long domainValue)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                lessThanOrEqual(IntegerType.INTEGER, domainValue)),
                false);

        return logicalPart.getPages(ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()), Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void testGetPagesMinMaxLessEqual()
    {
        LogicalPart logicalPart = setupMinMaxLogicalPart(PAGE_VALUES[0], PAGE_VALUES[PAGE_VALUES.length - 1]);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(PAGE_VALUES);
        addPages(logicalPart, pages);
        long max = PAGE_VALUES[PAGE_VALUES.length - 1];
        long min = PAGE_VALUES[0];

        // 2. domain value is greater than max
        long domainValue = max + 1L;
        result = getMinMaxLessEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 3. domain value is lesser than min
        domainValue = min - 1L;
        result = getMinMaxLessEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 4. domain value is max
        domainValue = max;
        result = getMinMaxLessEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 5. domain value is min
        domainValue = min;
        result = getMinMaxLessEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 6. domain value is within bounds and is a page value, on two pages
        domainValue = 3L;
        result = getMinMaxLessEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 7. domain value is within bounds and is a page value, on one page, a
        domainValue = 4L;
        result = getMinMaxLessEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 8. domain value is within bounds and is a page value, on one page, b
        domainValue = 5L;
        result = getMinMaxLessEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 9. domain value is within bounds and is not a page value
        domainValue = 6L;
        result = getMinMaxLessEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));
    }

    private List<Page> getMinMaxBetweenResult(LogicalPart logicalPart, Long low, Long high)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                range(IntegerType.INTEGER, low, true, high, true)),
                false);

        return logicalPart.getPages(ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()), Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void testGetPagesMinMaxBetween()
    {
        long[] pageValues = PAGE_VALUES;
        long[] newPageValues = {-20L, -20L, 20L, 20L, 20L, 30L, 30L, 40L, 50L, 70L, 70L, 70L};
        LogicalPart logicalPart = setupMinMaxLogicalPart(PAGE_VALUES[0], PAGE_VALUES[PAGE_VALUES.length - 1]);
        LogicalPart newLogicalPart = setupMinMaxLogicalPart(newPageValues[0], newPageValues[newPageValues.length - 1]);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(pageValues);
        addPages(logicalPart, pages);

        List<Page> newPages = buildFourByThreePagesList(newPageValues);
        addPages(newLogicalPart, newPages);

        // 2. low and high are greater than max
        long low = 100L;
        long high = 200L;
        result = getMinMaxBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        low = 100L;
        high = 200L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 3. low and high are less than min
        low = -200L;
        high = -100L;
        result = getMinMaxBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        low = -200L;
        high = -100L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 4. low is min high is max
        low = 2L;
        high = 7L;
        result = getMinMaxBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        low = -20L;
        high = 70L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        // 5. low is not min high is max, both are in pages
        low = 3L;
        high = 7L;
        result = getMinMaxBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        low = 4L;
        high = 7L;
        result = getMinMaxBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        low = 5L;
        high = 7L;
        result = getMinMaxBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        low = 20L;
        high = 70L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 30L;
        high = 70L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 40L;
        high = 70L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 50L;
        high = 70L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        // 5. low is not min, high is max, low is not in pages
        low = 6L;
        high = 7L;
        result = getMinMaxBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        low = -19L;
        high = 70L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 19L;
        high = 70L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 21L;
        high = 70L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 29L;
        high = 70L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 31L;
        high = 70L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 39L;
        high = 70L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 51L;
        high = 70L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        // low and high are within bounds and not in pages, no values between them are in pages
        low = -19L;
        high = 19L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 51L;
        high = 69L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        // low and high are within bounds and not in pages, some values between them are in pages
        low = -19L;
        high = 29L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = -19L;
        high = 51L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 31L;
        high = 51L;
        result = getMinMaxBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));
    }

    // Bloom index tests
    private LogicalPart setupBloomLogicalPart()
    {
        LogicalPart logicalPart = new LogicalPart(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Paths.get("/tmp"),
                mock(PageSorter.class),
                0,
                0,
                mock(TypeManager.class),
                mock(PagesSerde.class),
                0,
                false);

        AtomicReference<LogicalPart.LogicalPartState> processingState = new AtomicReference<>(LogicalPart.LogicalPartState.COMPLETED);
        ReflectionTestUtils.setField(logicalPart, "processingState", processingState);

        Set<Integer> indexChannels = new HashSet<>();
        indexChannels.add(0);
        ReflectionTestUtils.setField(logicalPart, "indexChannels", indexChannels);

        List<Integer> sortChannels = new ArrayList<>();
        sortChannels.add(0); // 0 is sorted
        ReflectionTestUtils.setField(logicalPart, "sortChannels", sortChannels);

        return logicalPart;
    }

    private List<Page> getBloomFilterEqualityResult(LogicalPart logicalPart, Long domainValue, boolean bloomFilterReturn)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                equal(IntegerType.INTEGER, domainValue)),
                false);

        Map<Integer, BloomFilter> bloomIdx = new HashMap<>();
        bloomIdx.put(0, mock(BloomFilter.class));

        ReflectionTestUtils.setField(logicalPart, "bloomIdx", bloomIdx);

        LogicalPart spyLogicalPart = spy(logicalPart);
        doReturn(bloomFilterReturn).when(spyLogicalPart).testFilter(Mockito.any(BloomFilter.class), eq(domainValue));

        return spyLogicalPart.getPages(Collections.emptyMap(),
                ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()), Collections.emptyMap());
    }

    @Test
    public void testGetPagesBloomEquality()
    {
        LogicalPart logicalPart = setupBloomLogicalPart();

        // 1. no pages
        List<Page> result = getBloomFilterEqualityResult(logicalPart, 3L, false);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        long[] pageValues = PAGE_VALUES;
        List<Page> pages = buildFourByThreePagesList(pageValues);
        addFourPages(logicalPart, pages);
        long max = pageValues[pageValues.length - 1];
        long min = pageValues[0];

        // 2. domain value is greater than max
        long domainValue = max + 1L;
        result = getBloomFilterEqualityResult(logicalPart, domainValue, false);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 3. domain value is lesser than min
        domainValue = min - 1L;
        result = getBloomFilterEqualityResult(logicalPart, domainValue, false);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 4. domain value is max
        domainValue = max;
        result = getBloomFilterEqualityResult(logicalPart, domainValue, true);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 5. domain value is min
        domainValue = min;
        result = getBloomFilterEqualityResult(logicalPart, domainValue, true);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 6a. domain value is within bounds and is a page value
        domainValue = 3L;
        result = getBloomFilterEqualityResult(logicalPart, domainValue, true);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 6b. domain value is within bounds and is a page value
        domainValue = 4L;
        result = getBloomFilterEqualityResult(logicalPart, domainValue, true);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 6c. domain value is within bounds and is a page value
        domainValue = 5L;
        result = getBloomFilterEqualityResult(logicalPart, domainValue, true);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 7. domain value is within bounds and is not a page value
        domainValue = 6L;
        result = getBloomFilterEqualityResult(logicalPart, domainValue, false);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));
    }

    // Spare Index Tests
    private TreeMap<Comparable, LogicalPart.SparseValue> getIndexedPagesMap(long[] pageValues)
    {
        TreeMap<Comparable, LogicalPart.SparseValue> indexedPagesMap = new TreeMap<>();

        for (int i = 0; i < pageValues.length; i += 3) {
            List<Integer> pageNumbers = new LinkedList<>();
            pageNumbers.add(i / 3);
            Comparable lastPageValue = pageValues[i + 2];
            int j = i + 3;
            // get all the pages after this one that start with the same value and should be in the same sparseidx
            // entry
            while (j < pageValues.length && pageValues[i] == pageValues[j]) {
                pageNumbers.add(j / 3);
                lastPageValue = pageValues[j + 2];
                j += 3;
            }

            Comparable finalLastPageValue = lastPageValue;
            indexedPagesMap.computeIfAbsent(pageValues[i], e -> new LogicalPart.SparseValue(pageNumbers, finalLastPageValue));

            // skip pages covered in while loop
            if (j > i + 3) {
                i = j - 3;
            }
        }

        return indexedPagesMap;
    }

    private LogicalPart setupSparseLogicalPart(long[] pageValues)
    {
        LogicalPart logicalPart = new LogicalPart(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Paths.get("/tmp"),
                mock(PageSorter.class),
                0,
                0,
                mock(TypeManager.class),
                mock(PagesSerde.class),
                0,
                false);

        AtomicReference<LogicalPart.LogicalPartState> processingState = new AtomicReference<>(LogicalPart.LogicalPartState.COMPLETED);
        ReflectionTestUtils.setField(logicalPart, "processingState", processingState);

        Set<Integer> indexChannels = new HashSet<>();
        indexChannels.add(0);
        ReflectionTestUtils.setField(logicalPart, "indexChannels", indexChannels);

        List<Integer> sortChannels = new ArrayList<>();
        sortChannels.add(0); // 0 is sorted
        ReflectionTestUtils.setField(logicalPart, "sortChannels", sortChannels);

        ReflectionTestUtils.setField(logicalPart, "sparseIdx", getIndexedPagesMap(pageValues));

        ReflectionTestUtils.setField(logicalPart, "types", TYPES);

        return logicalPart;
    }

    private void addFourPages(LogicalPart logicalPart, List<Page> pages)
    {
        ReflectionTestUtils.setField(logicalPart, "pages", pages);
    }

    private List<Page> getSparseFilterEqualityResult(LogicalPart logicalPart, Long domainValue)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                equal(IntegerType.INTEGER, domainValue)),
                false);

        return logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(),
                ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()));
    }

    @Test
    public void testGetPagesSparseEquality()
    {
        // setup LogicalPart
        long[] pageValues = PAGE_VALUES;
        LogicalPart logicalPart = setupSparseLogicalPart(pageValues);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(0, result.size());
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(pageValues);
        addFourPages(logicalPart, pages);

        // 2. domain value is greater than max
        long domainValue = 100L;
        result = getSparseFilterEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 3. domain value is lesser than min
        domainValue = 1L;
        result = getSparseFilterEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 4. domain value is max
        domainValue = 7L;
        result = getSparseFilterEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(3, 4)));

        // 5. domain value is min
        domainValue = 2L;
        result = getSparseFilterEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(0, 2)));

        // 6. domain value is within bounds, present in two pages, is the head of one
        domainValue = 3L;
        result = getSparseFilterEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(1, 3)));

        // 7. domain value is within bounds, present in one page, is the head of none
        domainValue = 4L;
        result = getSparseFilterEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(2, 3)));

        domainValue = 5L;
        result = getSparseFilterEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(2, 3)));

        //8. domain value is within bounds, not present in any pages
        domainValue = 6L;
        result = getSparseFilterEqualityResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));
    }

    private List<Page> getSparseFilterInResult(LogicalPart logicalPart, Long val1, Long val2)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                equal(IntegerType.INTEGER, val1),
                equal(IntegerType.INTEGER, val2)),
                false);

        return logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(),
                ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()));
    }

    @Test
    public void testGetPagesSparseIn()
    {
        // setup LogicalPart
        long[] pageValues = PAGE_VALUES;
        LogicalPart logicalPart = setupSparseLogicalPart(pageValues);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(0, result.size());
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(pageValues);
        addFourPages(logicalPart, pages);

        // 2. domain values are greater than max
        long domainValue1 = 100L;
        long domainValue2 = 200L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 3. domain values are less than min
        domainValue1 = -100L;
        domainValue2 = -200L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 4. domain values are max
        domainValue1 = 7L;
        domainValue2 = 7L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(3, 4)));

        // 5. domain values are min
        domainValue1 = 2L;
        domainValue2 = 2L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(0, 2)));

        // 6. domain values are the same and on one page
        domainValue1 = 4L;
        domainValue2 = 4L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(2, 3)));

        domainValue1 = 5L;
        domainValue2 = 5L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(2, 3)));

        // 7. domain values are different and on one page
        domainValue1 = 4L;
        domainValue2 = 5L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(2, 3)));

        // 8. domain values are different and on different pages
        domainValue1 = 2L;
        domainValue2 = 7L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 3);

        domainValue1 = 3L;
        domainValue2 = 7L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(1, 4)));

        domainValue1 = 4L;
        domainValue2 = 7L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(2, 4)));

        domainValue1 = 5L;
        domainValue2 = 7L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(2, 4)));

        // 9. domain values are the same and within range but not in any pages
        domainValue1 = 6L;
        domainValue2 = 6L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 10. one domain value is within bounds another is not, both are not on any pages
        domainValue1 = 6L;
        domainValue2 = 100L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 11. one domain value is within bounds another is not, one is a page value
        domainValue1 = 2L;
        domainValue2 = 100L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(0, 2)));

        domainValue1 = 3L;
        domainValue2 = 100L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(1, 3)));

        domainValue1 = 4L;
        domainValue2 = 100L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(2, 3)));

        domainValue1 = 5L;
        domainValue2 = 100L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(2, 3)));

        domainValue1 = 7L;
        domainValue2 = 100L;
        result = getSparseFilterInResult(logicalPart, domainValue1, domainValue2);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(3, 4)));
    }

    private List<Page> getSparseFilterGreaterResult(LogicalPart logicalPart, Long domainValue)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                greaterThan(IntegerType.INTEGER, domainValue)),
                false);

        return logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(),
                ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()));
    }

    @Test
    public void testGetPagesSparseGreater()
    {
        long[] pageValues = PAGE_VALUES;
        LogicalPart logicalPart = setupSparseLogicalPart(pageValues);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(0, result.size());
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(pageValues);
        addFourPages(logicalPart, pages);

        // 2. domain value is greater than max
        long domainValue = 100L;
        result = getSparseFilterGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 3. domain is less than min
        domainValue = 1L;
        result = getSparseFilterGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages)); // possible issue, does not return values in sorted, increasing order

        // 4. domain is max
        domainValue = 7L;
        result = getSparseFilterGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 5. domain is min
        domainValue = 2L;
        result = getSparseFilterGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(1, 4)));

        // 6. domain is within min and max and is a page value
        domainValue = 3L;
        result = getSparseFilterGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(2, 4)));

        //7. within min and max, is a page value, not head, has values larger than itself on its page
        domainValue = 4L;
        result = getSparseFilterGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(2, 4)));

        //8. within min and max, is a page value, not head, doesn't have values larger than itself on its page
        domainValue = 5L;
        result = getSparseFilterGreaterResult(logicalPart, domainValue);

        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(3, 4)));

        //9. within min and max, is not a page value
        domainValue = 6L;
        result = getSparseFilterGreaterResult(logicalPart, domainValue);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(3, 4)));
    }

    private List<Page> getSparseFilterGreaterOrEqualResult(LogicalPart logicalPart, Long domainValue)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                greaterThanOrEqual(IntegerType.INTEGER, domainValue)),
                false);

        return logicalPart.getPages(
                Collections.emptyMap(),
                Collections.emptyMap(),
                ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()));
    }

    @Test
    public void testGetPagesSparseGreaterOrEqual()
    {
        long[] pageValues = PAGE_VALUES;
        LogicalPart logicalPart = setupSparseLogicalPart(pageValues);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(0, result.size());
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(pageValues);
        addFourPages(logicalPart, pages);

        // 2. domain value is greater than max
        long domainValue = 100L;
        result = getSparseFilterGreaterOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 3. domain is less than min
        domainValue = 1L;
        result = getSparseFilterGreaterOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 4. domain is max
        domainValue = 7L;
        result = getSparseFilterGreaterOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(3, 4)));

        // 5. domain is min
        domainValue = 2L;
        result = getSparseFilterGreaterOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 6. domain is within min and max and is a page value
        domainValue = 3L;
        result = getSparseFilterGreaterOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(1, 4)));

        //7. within min and max, is a page value, not head, has values larger than itself on its page
        domainValue = 4L;
        result = getSparseFilterGreaterOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(2, 4)));

        //8. within min and max, is a page value, not head, doesn't have values larger than itself on its page
        domainValue = 5L;
        result = getSparseFilterGreaterOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(2, 4)));

        //9. within min and max, is not a page value
        domainValue = 6L;
        result = getSparseFilterGreaterOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(3, 4)));
    }

    private List<Page> getSparseFilterLessResult(LogicalPart logicalPart, Long domainValue)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                lessThan(IntegerType.INTEGER, domainValue)),
                false);

        List<Page> sparseResult = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(),
                ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()));

        return sparseResult;
    }

    @Test
    public void testGetPagesSparseLess()
    {
        long[] pageValues = PAGE_VALUES;
        LogicalPart logicalPart = setupSparseLogicalPart(pageValues);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(pageValues);
        addFourPages(logicalPart, pages);

        // 2. domain value is greater than max
        long domainValue = 100L;
        result = getSparseFilterLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 3. domain is less than min
        domainValue = 1L;
        result = getSparseFilterLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 4. domain is max
        domainValue = 7L;
        result = getSparseFilterLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));

        // 5. domain is min
        domainValue = 2L;
        result = getSparseFilterLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 6. domain is within min and max and is a page value
        domainValue = 3L;
        result = getSparseFilterLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(0, 2)));

        //7. within min and max, is a page value, not head, has values larger than itself on its page
        domainValue = 4L;
        result = getSparseFilterLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));

        //8. within min and max, is a page value, not head, doesn't have values larger than itself on its page
        domainValue = 5L;
        result = getSparseFilterLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));

        //9. within min and max, is not a page value
        domainValue = 6L;
        result = getSparseFilterLessResult(logicalPart, domainValue);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));
    }

    private List<Page> getSparseFilterLessOrEqualResult(LogicalPart logicalPart, Long domainValue)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                lessThanOrEqual(IntegerType.INTEGER, domainValue)),
                false);

        return logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(),
                ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()));
    }

    @Test
    public void testGetPagesSparseLessOrEqual()
    {
        long[] pageValues = PAGE_VALUES;
        LogicalPart logicalPart = setupSparseLogicalPart(pageValues);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(pageValues);
        addFourPages(logicalPart, pages);

        // 2. domain value is greater than max
        long domainValue = 100L;
        result = getSparseFilterLessOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 3. domain is less than min
        domainValue = 1L;
        result = getSparseFilterLessOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 4. domain is max
        domainValue = 7L;
        result = getSparseFilterLessOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        // 5. domain is min
        domainValue = 2L;
        result = getSparseFilterLessOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(0, 2)));

        // 6. domain is within min and max and is a page value
        domainValue = 3L;
        result = getSparseFilterLessOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));

        //7. within min and max, is a page value, not head, has values larger than itself on its page
        domainValue = 4L;
        result = getSparseFilterLessOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));

        //8. within min and max, is a page value, not head, doesn't have values larger than itself on its page
        domainValue = 5L;
        result = getSparseFilterLessOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));

        //9. within min and max, is not a page value
        domainValue = 6L;
        result = getSparseFilterLessOrEqualResult(logicalPart, domainValue);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));
    }

    private List<Page> getSparseFilterBetweenResult(LogicalPart logicalPart, Long low, Long high)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                range(IntegerType.INTEGER, low, true, high, true)),
                false);

        return logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(),
                ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()));
    }

    @Test
    public void testGetPagesSparseBetween()
    {
        long[] pageValues = PAGE_VALUES;
        LogicalPart logicalPart = setupSparseLogicalPart(pageValues);

        long[] newPageValues = {-20L, -20L, 20L, 20L, 20L, 30L, 30L, 40L, 50L, 70L, 70L, 70L};
        LogicalPart newLogicalPart = setupSparseLogicalPart(newPageValues);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(pageValues);
        addFourPages(logicalPart, pages);

        List<Page> newPages = buildFourByThreePagesList(newPageValues);
        addFourPages(newLogicalPart, newPages);

        // 2. low and high are greater than max
        long low = 100L;
        long high = 200L;
        result = getSparseFilterBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        low = 100L;
        high = 200L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 3. low and high are less than min
        low = -200L;
        high = -100L;
        result = getSparseFilterBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        low = -200L;
        high = -100L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 4. low is min high is max
        low = 2L;
        high = 7L;
        result = getSparseFilterBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, pages));

        low = -20L;
        high = 70L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        // 5. low is not min high is max, both are in pages
        low = 3L;
        high = 7L;
        result = getSparseFilterBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(1, 4)));

        low = 4L;
        high = 7L;
        result = getSparseFilterBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(2, 4)));

        low = 5L;
        high = 7L;
        result = getSparseFilterBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(2, 4)));

        low = 20L;
        high = 70L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 30L;
        high = 70L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(1, 4)));

        low = 40L;
        high = 70L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 4)));

        low = 50L;
        high = 70L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 4)));

        // 5. low is not min, high is max, low is not in pages
        low = 6L;
        high = 7L;
        result = getSparseFilterBetweenResult(logicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(3, 4)));

        low = -19L;
        high = 70L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 19L;
        high = 70L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        low = 21L;
        high = 70L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(1, 4)));

        low = 29L;
        high = 70L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(1, 4)));

        low = 31L;
        high = 70L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 4)));

        low = 39L;
        high = 70L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 4)));

        low = 51L;
        high = 70L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(3, 4)));

        // low and high are within bounds and not in pages, no values between them are in pages but could be
        low = -19L;
        high = 19L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 1)));

        low = 21L;
        high = 29L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(1, 2)));

        low = 33L;
        high = 34L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 3)));

        low = 42L;
        high = 47L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 3)));

        // low and high are within bounds and not in pages, no values between them could be in pages
        low = 51L;
        high = 59L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        low = 51L;
        high = 69L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // low and high are within bounds and not in pages, some values between them are in pages
        low = -19L;
        high = 29L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 2)));

        low = -19L;
        high = 51L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 3)));

        low = 31L;
        high = 51L;
        result = getSparseFilterBetweenResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 3)));
    }

    private List<Page> getSparseFilterNonInclusiveRangeResult(LogicalPart logicalPart, Long low, Long high)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                range(IntegerType.INTEGER, low, false, high, false)),
                false);

        return logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(),
                ImmutableMap.of(0, ((SortedRangeSet) domain.getValues()).getOrderedRanges()));
    }

    @Test
    public void testGetPagesSparseNonInclusiveRange()
    {
        long[] pageValues = PAGE_VALUES;
        LogicalPart logicalPart = setupSparseLogicalPart(pageValues);

        long[] newPageValues = {-20L, -20L, 20L, 20L, 20L, 30L, 30L, 40L, 50L, 70L, 70L, 70L};
        LogicalPart newLogicalPart = setupSparseLogicalPart(newPageValues);

        // 1. no pages
        List<Page> result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        result = logicalPart.getPages(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // setup pages
        List<Page> pages = buildFourByThreePagesList(pageValues);
        addFourPages(logicalPart, pages);

        List<Page> newPages = buildFourByThreePagesList(newPageValues);
        addFourPages(newLogicalPart, newPages);

        // 2. low and high are greater than max
        long low = 100L;
        long high = 200L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 3. low and high are lesser than min
        low = -200L;
        high = -100L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 4. low and high are min and max
        low = 2L;
        high = 7L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(1, 3)));

        low = -20L;
        high = 70L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 3)));

        // 4. low is lower than min, high is max
        low = -100L;
        high = 7L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));

        low = -100L;
        high = 70L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 3)));

        // 5. high is higher than max, low is min
        low = 2L;
        high = 100L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(1, 4)));

        low = -20L;
        high = 100L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 4);
        assertTrue(areListPagesEqual(result, newPages));

        // 6. low is within bounds and is a page value, high is greater than max
        low = 3L;
        high = 100L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(2, 4)));

        low = 4L;
        high = 100L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(2, 4)));

        low = 5L;
        high = 100L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(3, 4)));

        low = 20L;
        high = 100L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(1, 4)));

        low = 30L;
        high = 100L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 4)));

        low = 40L;
        high = 100L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 4)));

        low = 50L;
        high = 100L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(3, 4)));

        // 7. high is within bounds and is a page value, low is lower than min
        low = -100L;
        high = 5L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));

        low = -100L;
        high = 4L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));

        low = -100L;
        high = 3L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(0, 2)));

        low = -100L;
        high = 50L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 3)));

        low = -100L;
        high = 40L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 3)));

        low = -100L;
        high = 30L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 2)));

        low = -100L;
        high = 20L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 1)));

        // 8. low is within bounds and is a page value, high is max
        low = 3L;
        high = 7L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(2, 3)));

        low = 4L;
        high = 7L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(2, 3)));

        low = 5L;
        high = 7L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        low = 20L;
        high = 70L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(1, 3)));

        low = 30L;
        high = 70L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 3)));

        low = 40L;
        high = 70L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 3)));

        low = 50L;
        high = 70L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 0);
        assertTrue(areListPagesEqual(result, Collections.emptyList()));

        // 9. high is a page value, low is min
        low = 2L;
        high = 3L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(1, 2)));

        low = 2L;
        high = 4L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(1, 3)));

        low = 2L;
        high = 5L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(1, 3)));

        low = -20L;
        high = 20L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 1)));

        low = -20L;
        high = 30L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 2)));

        low = -20L;
        high = 40L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 3)));

        low = -20L;
        high = 50L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 3)));

        // 9. high is within bounds and is a page value, low is lower than min
        low = -100L;
        high = 5L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));

        low = -100L;
        high = 4L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, pages.subList(0, 3)));

        low = -100L;
        high = 3L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, pages.subList(0, 2)));

        low = -100L;
        high = 50L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 3)));

        low = -100L;
        high = 40L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 3);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 3)));

        low = -100L;
        high = 30L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 2)));

        low = -100L;
        high = 20L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(0, 1)));

        //10. low and high are page values, not max or min
        low = 3L;
        high = 5L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(2, 3)));

        low = 4L;
        high = 5L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(2, 3)));

        low = 3L;
        high = 4L;
        result = getSparseFilterNonInclusiveRangeResult(logicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, pages.subList(2, 3)));

        low = 20L;
        high = 50L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(1, 3)));

        low = 30L;
        high = 50L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 3)));

        low = 40L;
        high = 50L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(2, 3)));

        low = 20L;
        high = 30L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 1);
        assertTrue(areListPagesEqual(result, newPages.subList(1, 2)));

        low = 20L;
        high = 40L;
        result = getSparseFilterNonInclusiveRangeResult(newLogicalPart, low, high);
        assertEquals(result.size(), 2);
        assertTrue(areListPagesEqual(result, newPages.subList(1, 3)));
    }

    static class BlockComparator
            implements Comparator<long[]>
    {
        private int compareLong(long i, long j)
        {
            if (i - j < 0) {
                return -1;
            }
            else if (i - j == 0) {
                return 0;
            }
            else {
                return 1;
            }
        }

        public int compare(long[] a, long[] b)
        {
            int total = compareLong(a[0], b[0]) + compareLong(a[1], b[1]) + compareLong(a[2], b[2]);

            if (total > 0) {
                return 1;
            }
            else if (total < 0) {
                return -1;
            }
            else {
                return 0;
            }
        }
    }
}
