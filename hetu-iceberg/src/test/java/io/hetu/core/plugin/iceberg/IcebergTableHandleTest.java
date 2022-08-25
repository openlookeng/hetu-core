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
import io.airlift.units.DataSize;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RetryMode;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeSignature;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class IcebergTableHandleTest
{
    private IcebergTableHandle icebergTableHandleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        icebergTableHandleUnderTest = new IcebergTableHandle("schemaName", "tableName", TableType.DATA,
                Optional.of(0L), "tableSchemaJson", "partitionSpecJson", 0,
                TupleDomain.withColumnDomains(new HashMap<>()),
                TupleDomain.withColumnDomains(new HashMap<>()), new HashSet<>(
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new Type(),
                        Arrays.asList(0), new Type(), Optional.of("value")))),
                Optional.of("value"), "tableLocation", new HashMap<>(), RetryMode.NO_RETRIES,
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new Type(),
                        Arrays.asList(0), new Type(), Optional.of("value"))), false,
                Optional.of(DataSize.ofBytes(0L)));
    }

    public static class Type
            implements io.prestosql.spi.type.Type
    {
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
        public List<io.prestosql.spi.type.Type> getTypeParameters()
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
    }

    @Test
    public void testSetUpdateColumnHandle()
    {
        // Setup
        final List<IcebergColumnHandle> columnHandles = Arrays.asList(new IcebergColumnHandle(
                new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new Type(),
                Arrays.asList(0), new Type(), Optional.of("value")));

        // Run the test
        icebergTableHandleUnderTest.setUpdateColumnHandle(columnHandles);

        // Verify the results
    }

    @Test
    public void test()
    {
        icebergTableHandleUnderTest.getFormatVersion();
        icebergTableHandleUnderTest.getPartitionSpecJson();
        icebergTableHandleUnderTest.getRetryMode();
        icebergTableHandleUnderTest.getUpdatedColumns();
        icebergTableHandleUnderTest.getStorageProperties();
        icebergTableHandleUnderTest.getTableLocation();
        icebergTableHandleUnderTest.getSchemaName();
        icebergTableHandleUnderTest.getTableName();
        icebergTableHandleUnderTest.getTableType();
        icebergTableHandleUnderTest.getSnapshotId();
        icebergTableHandleUnderTest.getTableSchemaJson();
        icebergTableHandleUnderTest.getUnenforcedPredicate();
        icebergTableHandleUnderTest.getEnforcedPredicate();
        icebergTableHandleUnderTest.getProjectedColumns();
        icebergTableHandleUnderTest.getNameMappingJson();
        icebergTableHandleUnderTest.isRecordScannedFiles();
        icebergTableHandleUnderTest.getMaxScannedFileSize();
    }

    @Test
    public void testGetSchemaTableName()
    {
        // Setup
        final SchemaTableName expectedResult = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final SchemaTableName result = icebergTableHandleUnderTest.getSchemaTableName();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetSchemaTableNameWithType()
    {
        // Setup
        final SchemaTableName expectedResult = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final SchemaTableName result = icebergTableHandleUnderTest.getSchemaTableNameWithType();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testWithRetryMode()
    {
        // Setup
        // Run the test
        final IcebergTableHandle result = icebergTableHandleUnderTest.withRetryMode(RetryMode.NO_RETRIES);
    }

    @Test
    public void testWithUpdatedColumns()
    {
        // Setup
        final List<IcebergColumnHandle> updatedColumns = Arrays.asList(new IcebergColumnHandle(
                new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new Type(),
                Arrays.asList(0), new Type(), Optional.of("value")));
        final IcebergTableHandle expectedResult = new IcebergTableHandle("schemaName", "tableName", TableType.DATA,
                Optional.of(0L), "tableSchemaJson", "partitionSpecJson", 0,
                TupleDomain.withColumnDomains(new HashMap<>()), TupleDomain.withColumnDomains(new HashMap<>()),
                new HashSet<>(
                        Arrays.asList(new IcebergColumnHandle(
                                new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()),
                                new Type(), Arrays.asList(0), new Type(), Optional.of("value")))), Optional.of("value"),
                "tableLocation", new HashMap<>(), RetryMode.NO_RETRIES, Arrays.asList(new IcebergColumnHandle(new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new Type(),
                Arrays.asList(0), new Type(), Optional.of("value"))), false, Optional.of(
                DataSize.ofBytes(0L)));

        // Run the test
        final IcebergTableHandle result = icebergTableHandleUnderTest.withUpdatedColumns(updatedColumns);
    }

    @Test
    public void testWithProjectedColumns()
    {
        // Setup
        final Set<IcebergColumnHandle> projectedColumns = new HashSet<>(
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new Type(),
                        Arrays.asList(0), new Type(), Optional.of("value"))));
        final IcebergTableHandle expectedResult = new IcebergTableHandle("schemaName", "tableName", TableType.DATA,
                Optional.of(0L), "tableSchemaJson", "partitionSpecJson", 0,
                TupleDomain.withColumnDomains(new HashMap<>()), TupleDomain.withColumnDomains(new HashMap<>()),
                new HashSet<>(
                        Arrays.asList(new IcebergColumnHandle(
                                new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()),
                                new Type(), Arrays.asList(0), new Type(), Optional.of("value")))), Optional.of("value"),
                "tableLocation", new HashMap<>(), RetryMode.NO_RETRIES, Arrays.asList(new IcebergColumnHandle(new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new Type(),
                Arrays.asList(0), new Type(), Optional.of("value"))), false, Optional.of(
                DataSize.ofBytes(0L)));

        // Run the test
        final IcebergTableHandle result = icebergTableHandleUnderTest.withProjectedColumns(projectedColumns);
    }

    @Test
    public void testForOptimize()
    {
        // Setup
        final DataSize maxScannedFileSize = DataSize.ofBytes(0L);

        // Run the test
        final IcebergTableHandle result = icebergTableHandleUnderTest.forOptimize(false, maxScannedFileSize);
    }

    @Test
    public void testEquals()
    {
        IcebergTableHandle icebergTableHandle = new IcebergTableHandle("schemaName", "tableName", TableType.DATA, Optional.of(0L), "tableSchemaJson", "partitionSpecJson", 0, TupleDomain.withColumnDomains(new HashMap<>()), TupleDomain.withColumnDomains(new HashMap<>()), new HashSet<>(
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new Type(),
                        Arrays.asList(0), new Type(), Optional.of("value")))), Optional.of("value"), "tableLocation", new HashMap<>(), RetryMode.NO_RETRIES, Arrays.asList(new IcebergColumnHandle(
                                new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new Type(),
                Arrays.asList(0), new Type(), Optional.of("value"))));
        icebergTableHandleUnderTest.equals(icebergTableHandle);
    }

    @Test
    public void testHashCode()
    {
        assertEquals(0, icebergTableHandleUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = icebergTableHandleUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }
}
