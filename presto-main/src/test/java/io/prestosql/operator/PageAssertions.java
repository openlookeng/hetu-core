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

import io.prestosql.spi.Page;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.type.Type;

import java.util.List;

import static io.prestosql.block.BlockAssertions.assertBlockEquals;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public final class PageAssertions
{
    private PageAssertions()
    {
    }

    public static void assertPageEquals(List<? extends Type> types, Page actualPage, Page expectedPage)
    {
        if (expectedPage instanceof MarkerPage) {
            assertTrue(expectedPage instanceof MarkerPage);
            MarkerPage actual = (MarkerPage) actualPage;
            MarkerPage expected = (MarkerPage) expectedPage;
            assertEquals(actual.getSnapshotId(), expected.getSnapshotId());
            assertEquals(actual.isResuming(), expected.isResuming());
            return;
        }

        assertEquals(types.size(), actualPage.getChannelCount());
        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        for (int i = 0; i < actualPage.getChannelCount(); i++) {
            assertBlockEquals(types.get(i), actualPage.getBlock(i), expectedPage.getBlock(i));
        }
    }
}
