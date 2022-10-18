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
package io.prestosql.plugin.hive;

import io.prestosql.plugin.hive.util.FieldSetterFactoryTest;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static org.mockito.MockitoAnnotations.initMocks;

public class ReaderProjectionsAdapterTest
{
    @Mock
    private ReaderColumns mockReadColumns;

    private ReaderProjectionsAdapter readerProjectionsAdapterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        ReaderProjectionsAdapter.ProjectionGetter projectionGetter = new ReaderProjectionsAdapter.ProjectionGetter()
        {
            @Override
            public List<Integer> get(ColumnHandle required, ColumnHandle read)
            {
                return Arrays.asList(1);
            }
        };
        ReaderProjectionsAdapter.ColumnTypeGetter columnTypeGetter = new ReaderProjectionsAdapter.ColumnTypeGetter()
        {
            @Override
            public Type get(ColumnHandle column)
            {
                return new FieldSetterFactoryTest.Type();
            }
        };
        HiveColumnHandle hiveColumnHandle = new HiveColumnHandle("name", HIVE_STRING, new TypeSignature("base", TypeSignatureParameter.of(0L)), 0, HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false);
        List<HiveColumnHandle> hiveColumnHandles = Arrays.asList(hiveColumnHandle);
        readerProjectionsAdapterUnderTest = new ReaderProjectionsAdapter(hiveColumnHandles, new ReaderColumns(hiveColumnHandles, Arrays.asList(0)), columnTypeGetter, projectionGetter);
    }

    public static class Block
            implements io.prestosql.spi.block.Block
    {
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
            return 1;
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

        @Override
        public void retainedBytesForEachPart(BiConsumer consumer)
        {
        }
    }

    @Test
    public void testAdaptPage()
    {
        Page page = new Page(new Block());
        final Page result = readerProjectionsAdapterUnderTest.adaptPage(page);

        // Verify the results
    }

    @Test
    public void testGetOutputToInputMapping()
    {
        // Setup
        // Run the test
        final List<ReaderProjectionsAdapter.ChannelMapping> result = readerProjectionsAdapterUnderTest.getOutputToInputMapping();

        // Verify the results
    }

    @Test
    public void testGetOutputTypes()
    {
        // Setup
        // Run the test
        final List<Type> result = readerProjectionsAdapterUnderTest.getOutputTypes();

        // Verify the results
    }

    @Test
    public void testGetInputTypes()
    {
        // Setup
        // Run the test
        final List<Type> result = readerProjectionsAdapterUnderTest.getInputTypes();

        // Verify the results
    }
}
