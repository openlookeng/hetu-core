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

import com.google.common.collect.ImmutableList;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.SnapshotTestUtil;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.TestingPagesSerdeFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.SequencePageBuilder.createSequencePage;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPagesIndex
{
    @Test
    public void testEstimatedSizeSnapshot()
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR);
        PagesSerde serde = TestingPagesSerdeFactory.testingPagesSerde();
        Page input = somePage(types);

        PagesIndex pagesIndex = newPagesIndex(types, 30, false);
        long initialEstimatedSize = pagesIndex.getEstimatedSize().toBytes();
        assertTrue(initialEstimatedSize > 0, format("Initial estimated size must be positive, got %s", initialEstimatedSize));

        pagesIndex.addPage(input);
        long estimatedSizeWithOnePage = pagesIndex.getEstimatedSize().toBytes();
        assertTrue(estimatedSizeWithOnePage > initialEstimatedSize, "Estimated size should grow after adding a page");

        Object snapshot = pagesIndex.capture(serde);
        assertEquals(SnapshotTestUtil.toSimpleSnapshotMapping(snapshot), createExpectedMapping());

        pagesIndex.addPage(input);
        long estimatedSizeWithTwoPages = pagesIndex.getEstimatedSize().toBytes();
        assertEquals(
                estimatedSizeWithTwoPages,
                initialEstimatedSize + (estimatedSizeWithOnePage - initialEstimatedSize) * 2,
                "Estimated size should grow linearly as long as we don't pass expectedPositions");

        pagesIndex.compact();
        long estimatedSizeAfterCompact = pagesIndex.getEstimatedSize().toBytes();
        // We can expect compact to reduce size because VARCHAR sequence pages are compactable.
        assertTrue(estimatedSizeAfterCompact < estimatedSizeWithTwoPages, format(
                "Compact should reduce (or retain) size, but changed from %s to %s",
                estimatedSizeWithTwoPages,
                estimatedSizeAfterCompact));
    }

    private Map<String, Object> createExpectedMapping()
    {
        Map<String, Object> expectedMapping = new HashMap<>();
        List<Long> valueAddresses = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            valueAddresses.add((long) i);
        }
        expectedMapping.put("valueAddresses", valueAddresses);
        expectedMapping.put("nextBlockToCompact", 0);
        expectedMapping.put("positionCount", 7);
        expectedMapping.put("pagesMemorySize", 3860L);
        expectedMapping.put("estimatedSize", 12404L);
        return expectedMapping;
    }

    @Test
    public void testEagerCompact()
    {
        List<Type> types = ImmutableList.of(VARCHAR);

        PagesIndex lazyCompactPagesIndex = newPagesIndex(types, 50, false);
        PagesIndex eagerCompactPagesIndex = newPagesIndex(types, 50, true);

        for (int i = 0; i < 5; i++) {
            lazyCompactPagesIndex.addPage(somePage(types));
            eagerCompactPagesIndex.addPage(somePage(types));

            // We can expect eagerCompactPagesIndex retained less data than lazyCompactPagesIndex because
            // the pages used in the test (VARCHAR sequence pages) are compactable.
            assertTrue(
                    eagerCompactPagesIndex.getEstimatedSize().toBytes() < lazyCompactPagesIndex.getEstimatedSize().toBytes(),
                    "Expect eagerCompactPagesIndex retained less data than lazyCompactPagesIndex after adding the page, because the pages used in the test are compactable.");
        }

        lazyCompactPagesIndex.compact();
        assertEquals(lazyCompactPagesIndex.getEstimatedSize(), eagerCompactPagesIndex.getEstimatedSize());
    }

    private static PagesIndex newPagesIndex(List<Type> types, int expectedPositions, boolean eagerCompact)
    {
        return new PagesIndex.TestingFactory(eagerCompact).newPagesIndex(types, expectedPositions);
    }

    private static Page somePage(List<Type> types)
    {
        int[] initialValues = new int[types.size()];
        Arrays.setAll(initialValues, i -> 100 * i);
        return createSequencePage(types, 7, initialValues);
    }
}
