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
package io.prestosql.plugin.base.type;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.type.TimestampType;
import org.joda.time.DateTimeZone;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ShortTimestampEncoderTest
{
    @Mock
    private DateTimeZone mockTimeZone;

    private ShortTimestampEncoder shortTimestampEncoderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        shortTimestampEncoderUnderTest = new ShortTimestampEncoder(TimestampType.TIMESTAMP,
                mockTimeZone);
    }

    @Test
    public void testWrite() throws Exception
    {
        // Setup
        final DecodedTimestamp decodedTimestamp = new DecodedTimestamp(11L, 11);
        final BlockBuilder blockBuilder = new BlockBuilder() {
            @Override
            public BlockBuilder closeEntry()
            {
                return null;
            }

            @Override
            public BlockBuilder appendNull()
            {
                return null;
            }

            @Override
            public Block build()
            {
                return null;
            }

            @Override
            public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
            {
                return null;
            }

            @Override
            public void writePositionTo(int position, BlockBuilder blockBuilder)
            {
            }

            @Override
            public Block getSingleValueBlock(int position)
            {
                return null;
            }

            @Override
            public int getPositionCount()
            {
                return 0;
            }

            @Override
            public long getSizeInBytes()
            {
                return 0;
            }

            @Override
            public long getRegionSizeInBytes(int position, int length)
            {
                return 0;
            }

            @Override
            public long getPositionsSizeInBytes(boolean[] positions)
            {
                return 0;
            }

            @Override
            public long getRetainedSizeInBytes()
            {
                return 0;
            }

            @Override
            public long getEstimatedDataSizeForStats(int position)
            {
                return 0;
            }

            @Override
            public void retainedBytesForEachPart(BiConsumer consumer)
            {
            }

            @Override
            public String getEncodingName()
            {
                return "string";
            }

            @Override
            public Block copyPositions(int[] positions, int offset, int length)
            {
                return null;
            }

            @Override
            public Block getRegion(int positionOffset, int length)
            {
                return null;
            }

            @Override
            public Block copyRegion(int position, int length)
            {
                return null;
            }

            @Override
            public boolean isNull(int position)
            {
                return false;
            }
        };
        when(mockTimeZone.convertUTCToLocal(0L)).thenReturn(0L);

        // Run the test
        shortTimestampEncoderUnderTest.write(decodedTimestamp, blockBuilder);

        // Verify the results
    }
}
