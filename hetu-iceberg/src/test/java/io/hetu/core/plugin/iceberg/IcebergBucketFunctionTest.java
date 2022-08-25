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
package io.hetu.core.plugin.iceberg;

import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.spi.type.TypeSignature;
import org.apache.iceberg.PartitionSpec;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class IcebergBucketFunctionTest
{
    @Mock
    private TypeOperators mockTypeOperators;
    @Mock
    private PartitionSpec mockPartitionSpec;

    private IcebergBucketFunction icebergBucketFunctionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        Type type = new Type() {
            @Override
            public TypeSignature getTypeSignature()
            {
                return null;
            }

            @Override
            public String getDisplayName()
            {
                return null;
            }

            @Override
            public boolean isComparable()
            {
                return false;
            }

            @Override
            public boolean isOrderable()
            {
                return false;
            }

            @Override
            public Class<?> getJavaType()
            {
                return null;
            }

            @Override
            public List<Type> getTypeParameters()
            {
                return null;
            }

            @Override
            public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
            {
                return null;
            }

            @Override
            public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
            {
                return null;
            }

            @Override
            public <T> Object getObjectValue(ConnectorSession session, Block<T> block, int position)
            {
                return null;
            }

            @Override
            public boolean getBoolean(Block block, int position)
            {
                return false;
            }

            @Override
            public long getLong(Block block, int position)
            {
                return 0;
            }

            @Override
            public double getDouble(Block block, int position)
            {
                return 0;
            }

            @Override
            public Slice getSlice(Block block, int position)
            {
                return null;
            }

            @Override
            public <T> Object getObject(Block<T> block, int position)
            {
                return null;
            }

            @Override
            public void writeBoolean(BlockBuilder blockBuilder, boolean value)
            {
            }

            @Override
            public void writeLong(BlockBuilder blockBuilder, long value)
            {
            }

            @Override
            public void writeDouble(BlockBuilder blockBuilder, double value)
            {
            }

            @Override
            public void writeSlice(BlockBuilder blockBuilder, Slice value)
            {
            }

            @Override
            public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
            {
            }

            @Override
            public void writeObject(BlockBuilder blockBuilder, Object value)
            {
            }

            @Override
            public void appendTo(Block block, int position, BlockBuilder blockBuilder)
            {
            }

            @Override
            public <T> boolean equalTo(Block<T> leftBlock, int leftPosition, Block<T> rightBlock, int rightPosition)
            {
                return false;
            }

            @Override
            public <T> long hash(Block<T> block, int position)
            {
                return 0;
            }

            @Override
            public <T> int compareTo(Block<T> leftBlock, int leftPosition, Block<T> rightBlock, int rightPosition)
            {
                return 0;
            }
        };
        icebergBucketFunctionUnderTest = new IcebergBucketFunction(mockTypeOperators, mockPartitionSpec,
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), type,
                        Arrays.asList(0), type, Optional.of("value"))), 1);
    }

    @Test
    public void testGetBucket()
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);

        // Run the test
        final int result = icebergBucketFunctionUnderTest.getBucket(page, 0);

        // Verify the results
        assertEquals(0, result);
    }
}
