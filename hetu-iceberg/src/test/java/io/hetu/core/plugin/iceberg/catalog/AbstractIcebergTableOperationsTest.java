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
package io.hetu.core.plugin.iceberg.catalog;

import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class AbstractIcebergTableOperationsTest
{
    @Mock
    private FileIO mockFileIo;
    @Mock
    private ConnectorSession mockSession;

    private AbstractIcebergTableOperations abstractIcebergTableOperationsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        abstractIcebergTableOperationsUnderTest = new AbstractIcebergTableOperations(mockFileIo, mockSession,
                "database", "table",
                Optional.of("value"), Optional.of("value")) {
            @Override
            protected String getRefreshedLocation()
            {
                return null;
            }

            @Override
            protected void commitNewTable(TableMetadata metadata)
            {
            }

            @Override
            protected void commitToExistingTable(TableMetadata base, TableMetadata metadata)
            {
            }

            @Override
            public EncryptionManager encryption()
            {
                return null;
            }

            @Override
            public TableOperations temp(TableMetadata uncommittedMetadata)
            {
                return null;
            }

            @Override
            public long newSnapshotId()
            {
                return 0;
            }
        };
    }

    @Test
    public void testInitializeFromMetadata()
    {
        // Setup
        final TableMetadata tableMetadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());

        // Run the test
        abstractIcebergTableOperationsUnderTest.initializeFromMetadata(tableMetadata);

        // Verify the results
    }

    @Test
    public void testCurrent()
    {
        // Setup
        // Run the test
        final TableMetadata result = abstractIcebergTableOperationsUnderTest.current();

        // Verify the results
    }

    @Test
    public void testRefresh()
    {
        // Setup
        // Run the test
        final TableMetadata result = abstractIcebergTableOperationsUnderTest.refresh();

        // Verify the results
    }

    @Test
    public void testCommit()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());

        // Run the test
        abstractIcebergTableOperationsUnderTest.commit(base, metadata);

        // Verify the results
    }

    @Test
    public void testIo()
    {
        // Setup
        // Run the test
        final FileIO result = abstractIcebergTableOperationsUnderTest.io();

        // Verify the results
    }

    @Test
    public void testMetadataFileLocation1()
    {
        // Setup
        // Run the test
        final String result = abstractIcebergTableOperationsUnderTest.metadataFileLocation("filename");

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testLocationProvider()
    {
        // Setup
        // Run the test
        final LocationProvider result = abstractIcebergTableOperationsUnderTest.locationProvider();

        // Verify the results
    }

    @Test
    public void testGetSchemaTableName()
    {
        // Setup
        final SchemaTableName expectedResult = new SchemaTableName("database", "table");

        // Run the test
        final SchemaTableName result = abstractIcebergTableOperationsUnderTest.getSchemaTableName();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testWriteNewMetadata()
    {
        // Setup
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);

        // Run the test
        final String result = abstractIcebergTableOperationsUnderTest.writeNewMetadata(metadata, 0);

        // Verify the results
        assertEquals("path", result);
    }

    @Test
    public void testRefreshFromMetadataLocation()
    {
        // Setup
        // Run the test
        abstractIcebergTableOperationsUnderTest.refreshFromMetadataLocation("newLocation");

        // Verify the results
    }

    @Test
    public void testNewTableMetadataFilePath()
    {
        // Setup
        final TableMetadata meta = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());

        // Run the test
        final String result = AbstractIcebergTableOperations.newTableMetadataFilePath(meta, 0);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testMetadataFileLocation2()
    {
        // Setup
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());

        // Run the test
        final String result = AbstractIcebergTableOperations.metadataFileLocation(metadata, "filename");

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testParseVersion()
    {
        assertEquals(0, AbstractIcebergTableOperations.parseVersion("metadataLocation"));
    }

    @Test
    public void testToHiveColumns()
    {
        // Setup
        final List<Types.NestedField> columns = Arrays.asList(Types.NestedField.optional(0, "name", null));
        final List<Column> expectedResult = Arrays.asList(
                new Column("name", HiveType.valueOf("hiveTypeName"), Optional.of("value")));

        // Run the test
        final List<Column> result = AbstractIcebergTableOperations.toHiveColumns(columns);

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
