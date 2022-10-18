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
package io.prestosql.parquet.writer;

import io.prestosql.parquet.writer.repdef.DefLevelIterable;
import io.prestosql.parquet.writer.repdef.RepLevelIterable;
import io.prestosql.spi.block.Block;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;

public class ColumnChunkTest
{
    @Mock
    private Block mockBlock;

    private ColumnChunk columnChunkUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        columnChunkUnderTest = new ColumnChunk(mockBlock, Arrays.asList(), Arrays.asList());
    }

    @Test
    public void testGetDefLevelIterables()
    {
        // Setup
        // Run the test
        final List<DefLevelIterable> result = columnChunkUnderTest.getDefLevelIterables();

        // Verify the results
    }

    @Test
    public void testGetRepLevelIterables()
    {
        // Setup
        // Run the test
        final List<RepLevelIterable> result = columnChunkUnderTest.getRepLevelIterables();

        // Verify the results
    }
}
