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
package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.prestosql.PagesIndexPageSorter;
import io.prestosql.RowPagesBuilder;
import io.prestosql.operator.PagesIndex;
import io.prestosql.plugin.memory.data.SortBuffer;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.OperatorAssertion.toMaterializedResult;
import static io.prestosql.spi.block.SortOrder.ASC_NULLS_FIRST;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestSortBuffer
{
    private static final PagesIndexPageSorter sorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));

    @Test
    public void testPageSorter()
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARCHAR);
        List<Integer> sortChannels = Ints.asList(0);
        List<SortOrder> sortOrders = ImmutableList.of(ASC_NULLS_FIRST);

        SortBuffer sortBuffer = new SortBuffer(
                new DataSize(1, DataSize.Unit.GIGABYTE),
                types,
                sortChannels,
                sortOrders,
                sorter);

        List<Page> inputPages = RowPagesBuilder.rowPagesBuilder(types)
                .row(2L, 1.1, "d")
                .row(1L, 2.2, "c")
                .pageBreak()
                .row(-2L, 2.2, "b")
                .row(-12L, 2.2, "a")
                .build();

        List<Page> expectedPages = RowPagesBuilder.rowPagesBuilder(types)
                .row(-12L, 2.2, "a")
                .row(-2L, 2.2, "b")
                .row(1L, 2.2, "c")
                .row(2L, 1.1, "d")
                .build();

        inputPages.stream().forEach(sortBuffer::add);
        List<Page> actualPages = new LinkedList<>();
        sortBuffer.flushTo(actualPages::add);

        assertSorted(expectedPages, actualPages, types);
    }

    @Test
    public void testPageSorterSmallPages()
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARCHAR);
        List<Integer> sortChannels = Ints.asList(0);
        List<SortOrder> sortOrders = ImmutableList.of(ASC_NULLS_FIRST);

        SortBuffer sortBuffer = new SortBuffer(
                new DataSize(1, DataSize.Unit.GIGABYTE),
                types,
                sortChannels,
                sortOrders,
                sorter);

        RowPagesBuilder inputPagesBuilder = RowPagesBuilder.rowPagesBuilder(types);
        for (long i = 1; i < 100000; i++) {
            inputPagesBuilder.row(i, 1.1, "a").pageBreak();
        }
        List<Page> inputPages = inputPagesBuilder.build();

        RowPagesBuilder expectedPagesBuilder = RowPagesBuilder.rowPagesBuilder(types);
        for (long i = 1; i < 100000; i++) {
            expectedPagesBuilder.row(i, 1.1, "a");

            // based on default max page size, each page will be full when it has 43691 rows
            if (i % 43691 == 0) {
                expectedPagesBuilder.pageBreak();
            }
        }
        List<Page> expectedPages = expectedPagesBuilder.build();

        inputPages.stream().forEach(sortBuffer::add);
        List<Page> actualPages = new LinkedList<>();
        sortBuffer.flushTo(actualPages::add);

        assertSorted(expectedPages, actualPages, types);
    }

    private static void assertSorted(List<Page> expectedPages, List<Page> actualPages, List<Type> types)
    {
        assertEquals(expectedPages.size(), actualPages.size());

        MaterializedResult expected = toMaterializedResult(TEST_SESSION, types, expectedPages);
        MaterializedResult actual = toMaterializedResult(TEST_SESSION, types, actualPages);
        assertEquals(expected.getMaterializedRows(), actual.getMaterializedRows());
    }
}
