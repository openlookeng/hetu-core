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
package io.hetu.core.plugin.iceberg.delete;

import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;

public class TrinoRowTest
{
    private static final int POSITIONS_PER_PAGE = 0;

    @Test
    public void testFromPage()
    {
        // Setup
        final Type[] types = new Type[]{};
        Block build = BIGINT.createFixedSizeBlockBuilder(POSITIONS_PER_PAGE).build();
        final Page page = new Page(0, build);

        // Run the test
        Iterable<TrinoRow> trinoRows = TrinoRow.fromPage(types, page, 0);

        // Verify the results
    }
}
