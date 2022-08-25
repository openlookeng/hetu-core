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
import io.hetu.core.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalog;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveBucketing;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static org.testng.Assert.assertEquals;

public class IcebergUtilTest
{
    @Test
    public void testIsIcebergTable()
    {
        // Setup
        final Table table = new Table("databaseName", "tableName", "value", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("name", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("name", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));

        // Run the test
        final boolean result = IcebergUtil.isIcebergTable(table);
    }

    @Test
    public void testLoadIcebergTable()
    {
        // Setup
        final TrinoCatalog catalog = null;
        final IcebergTableOperationsProvider tableOperationsProvider = null;
        final ConnectorSession session = null;
        final SchemaTableName table = new SchemaTableName("schemaName", "tableName");

        // Run the test
        IcebergUtil.loadIcebergTable(catalog, tableOperationsProvider, session, table);

        // Verify the results
    }

    @Test
    public void testGetIcebergTableWithMetadata()
    {
        // Setup
        final TrinoCatalog catalog = null;
        final IcebergTableOperationsProvider tableOperationsProvider = null;
        final ConnectorSession session = null;
        final SchemaTableName table = new SchemaTableName("schemaName", "tableName");
        final TableMetadata tableMetadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());

        // Run the test
        final org.apache.iceberg.Table result = IcebergUtil.getIcebergTableWithMetadata(catalog,
                tableOperationsProvider, session, table, tableMetadata);

        // Verify the results
    }

    @Test
    public void testResolveSnapshotId()
    {
        // Setup
        TableOperations tableOperations = new TableOperations()
        {
            @Override
            public TableMetadata current()
            {
                HashMap<String, String> stringStringHashMap = new HashMap<>();
                stringStringHashMap.put("key", "value");
                TableMetadata location = TableMetadata.newTableMetadata(new Schema(), PartitionSpec.unpartitioned(), SortOrder.unsorted(), "location", stringStringHashMap);
                TableMetadata.buildFrom(location);
                return location;
            }

            @Override
            public TableMetadata refresh()
            {
                return null;
            }

            @Override
            public void commit(TableMetadata base, TableMetadata metadata)
            {
            }

            @Override
            public FileIO io()
            {
                return null;
            }

            @Override
            public String metadataFileLocation(String fileName)
            {
                return null;
            }

            @Override
            public LocationProvider locationProvider()
            {
                return null;
            }
        };
        final org.apache.iceberg.Table table = new BaseTable(tableOperations, "name");

        // Run the test
        final long result = IcebergUtil.resolveSnapshotId(table, 0L);
    }

    @Test
    public void testGetColumns()
    {
        // Setup
        final Schema schema = new Schema(0, Arrays.asList(Types.NestedField.optional(0, "name", new Types.BooleanType())), new HashMap<>(),
                new HashSet<>(Arrays.asList(0)));
        final TypeManager typeManager = null;
        io.prestosql.spi.type.Type type = new io.prestosql.spi.type.Type() {
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
        };
        final List<IcebergColumnHandle> expectedResult = Arrays.asList(new IcebergColumnHandle(
                new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), type,
                Arrays.asList(0), type, Optional.of("value")));

        // Run the test
        IcebergUtil.getColumns(schema, typeManager);
    }

    @Test
    public void testGetColumnHandle()
    {
        // Setup
        final Types.NestedField column = Types.NestedField.optional(0, "name", null);
        final TypeManager typeManager = null;
        final IcebergColumnHandle expectedResult = new IcebergColumnHandle(
                new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), null,
                Arrays.asList(0), null, Optional.of("value"));

        // Run the test
        final IcebergColumnHandle result = IcebergUtil.getColumnHandle(column, typeManager);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetIdentityPartitions()
    {
        // Setup
        final PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
        final Map<PartitionField, Integer> expectedResult = new HashMap<>();

        // Run the test
        final Map<PartitionField, Integer> result = IcebergUtil.getIdentityPartitions(partitionSpec);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testPrimitiveFieldTypes1()
    {
        // Setup
        final Schema schema = new Schema(0, Arrays.asList(Types.NestedField.optional(0, "name", null)), new HashMap<>(),
                new HashSet<>(
                        Arrays.asList(0)));

        // Run the test
        final Map<Integer, Type.PrimitiveType> result = IcebergUtil.primitiveFieldTypes(schema);

        // Verify the results
    }

    @Test
    public void testGetFileFormat()
    {
        // Setup
        final org.apache.iceberg.Table table = null;

        // Run the test
        final IcebergFileFormat result = IcebergUtil.getFileFormat(table);

        // Verify the results
        assertEquals(IcebergFileFormat.ORC, result);
    }

    @Test
    public void testGetTableComment()
    {
        // Setup
        final org.apache.iceberg.Table table = null;

        // Run the test
        final Optional<String> result = IcebergUtil.getTableComment(table);

        // Verify the results
        assertEquals(Optional.of("value"), result);
    }

    @Test
    public void testQuotedTableName()
    {
        // Setup
        final SchemaTableName name = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final String result = IcebergUtil.quotedTableName(name);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testDeserializePartitionValue()
    {
        // Setup
        final io.prestosql.spi.type.Type type = null;

        // Run the test
        final Object result = IcebergUtil.deserializePartitionValue(type, "valueString", "name");

        // Verify the results
    }

    @Test
    public void testGetPartitionKeys1()
    {
        // Setup
        final FileScanTask scanTask = null;
        final Map<Integer, Optional<String>> expectedResult = new HashMap<>();

        // Run the test
        final Map<Integer, Optional<String>> result = IcebergUtil.getPartitionKeys(scanTask);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetPartitionKeys2()
    {
        // Setup
        final StructLike partition = null;
        final PartitionSpec spec = PartitionSpec.unpartitioned();
        final Map<Integer, Optional<String>> expectedResult = new HashMap<>();

        // Run the test
        final Map<Integer, Optional<String>> result = IcebergUtil.getPartitionKeys(partition, spec);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetLocationProvider()
    {
        // Setup
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        final Map<String, String> storageProperties = new HashMap<>();

        // Run the test
        final LocationProvider result = IcebergUtil.getLocationProvider(schemaTableName, "tableLocation",
                storageProperties);

        // Verify the results
    }

    @Test
    public void testToIcebergSchema()
    {
        // Setup
        final List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false));

        // Run the test
        final Schema result = IcebergUtil.toIcebergSchema(columns);

        // Verify the results
    }

    @Test
    public void testNewCreateTableTransaction()
    {
        // Setup
        final TrinoCatalog catalog = null;
        final ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schemaName", "tableName"), Arrays.asList(
                        new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)),
                new HashMap<>(), Optional.of("value"), Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), Optional.of(new HashSet<>(
                Arrays.asList("value"))));
        final ConnectorSession session = null;

        // Run the test
        final Transaction result = IcebergUtil.newCreateTableTransaction(catalog, tableMetadata, session);

        // Verify the results
    }

    @Test
    public void testValidateTableCanBeDropped()
    {
        // Setup
        final org.apache.iceberg.Table table = null;

        // Run the test
        IcebergUtil.validateTableCanBeDropped(table);

        // Verify the results
    }
}
