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
package io.prestosql.parquet.reader;

import io.prestosql.spi.block.Block;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;

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
        columnChunkUnderTest = new ColumnChunk(mockBlock, new int[]{0}, new int[]{0});
    }
}
