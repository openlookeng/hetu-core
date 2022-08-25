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
package io.hetu.core.plugin.iceberg.catalog.hms;

import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.HiveBucketing;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.PrincipalPrivileges;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.connector.ConnectorSession;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class AbstractMetastoreTableOperationsTest
{
    @Mock
    private FileIO mockFileIo;
    @Mock
    private HiveMetastore mockMetastore;
    @Mock
    private ConnectorSession mockSession;

    private AbstractMetastoreTableOperations abstractMetastoreTableOperationsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        abstractMetastoreTableOperationsUnderTest = new AbstractMetastoreTableOperations(mockFileIo, mockMetastore,
                mockSession, "database", "table",
                Optional.of("value"), Optional.of("value")) {
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
    public void testGetRefreshedLocation()
    {
        // Setup
        // Configure HiveMetastore.getTable(...).
        final Optional<Table> table = Optional.of(
                new Table("databaseName", "tableName", "value", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("name", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        Arrays.asList(new Column("name", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value")));
        when(mockMetastore.getTable(new HiveIdentity((ConnectorSession) null), "database", "table")).thenReturn(table);

        // Run the test
        final String result = abstractMetastoreTableOperationsUnderTest.getRefreshedLocation();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testGetRefreshedLocation_HiveMetastoreReturnsAbsent()
    {
        // Setup
        when(mockMetastore.getTable(new HiveIdentity((ConnectorSession) null), "database", "table")).thenReturn(Optional.empty());

        // Run the test
        final String result = abstractMetastoreTableOperationsUnderTest.getRefreshedLocation();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testCommitNewTable()
    {
        // Setup
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", null)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        when(mockFileIo.newOutputFile("path")).thenReturn(null);

        // Run the test
        abstractMetastoreTableOperationsUnderTest.commitNewTable(metadata);

        // Verify the results
        verify(mockMetastore).createTable(
                eq(new Table("databaseName", "tableName", "value", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("name", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        Arrays.asList(new Column("name", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value"))),
                any(PrincipalPrivileges.class));
    }

    @Test
    public void testGetTable()
    {
        // Setup
        final Table expectedResult = new Table("databaseName", "tableName", "value", "tableType",
                new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                Arrays.asList(new Column("name", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                Arrays.asList(new Column("name", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));

        // Configure HiveMetastore.getTable(...).
        final Optional<Table> table = Optional.of(
                new Table("databaseName", "tableName", "value", "tableType", new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                        Arrays.asList(new Column("name", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        Arrays.asList(new Column("name", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                        new HashMap<>(), Optional.of("value"), Optional.of("value")));
        when(mockMetastore.getTable(new HiveIdentity((ConnectorSession) null), "database", "table")).thenReturn(table);

        // Run the test
        final Table result = abstractMetastoreTableOperationsUnderTest.getTable();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetTable_HiveMetastoreReturnsAbsent()
    {
        // Setup
        final Table expectedResult = new Table("databaseName", "tableName", "value", "tableType",
                new Storage(
                        StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                        Optional.of(new HiveBucketProperty(
                                Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                        new HashMap<>()),
                Arrays.asList(new Column("name", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                Arrays.asList(new Column("name", HiveType.valueOf("hiveTypeName"), Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        when(mockMetastore.getTable(new HiveIdentity((ConnectorSession) null), "database", "table")).thenReturn(Optional.empty());

        // Run the test
        final Table result = abstractMetastoreTableOperationsUnderTest.getTable();

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
