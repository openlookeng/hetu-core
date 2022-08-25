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

import io.airlift.units.DataSize;
import io.hetu.core.plugin.iceberg.ColumnIdentity;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.PrestoPrincipal;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.types.Types;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class AbstractTrinoCatalogTest
{
    @Mock
    private IcebergTableOperationsProvider mockTableOperationsProvider;

    private AbstractTrinoCatalog abstractTrinoCatalogUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        abstractTrinoCatalogUnderTest = new AbstractTrinoCatalog(mockTableOperationsProvider, "trinoVersion", false) {
            @Override
            public List<String> listNamespaces(ConnectorSession session)
            {
                return null;
            }

            @Override
            public void dropNamespace(ConnectorSession session, String namespace)
            {
            }

            @Override
            public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
            {
                return null;
            }

            @Override
            public Optional<PrestoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
            {
                return null;
            }

            @Override
            public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, PrestoPrincipal owner)
            {
            }

            @Override
            public void setNamespacePrincipal(ConnectorSession session, String namespace, PrestoPrincipal principal)
            {
            }

            @Override
            public void renameNamespace(ConnectorSession session, String source, String target)
            {
            }

            @Override
            public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
            {
                return null;
            }

            @Override
            public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, String location, Map<String, String> properties)
            {
                return null;
            }

            @Override
            public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
            {
            }

            @Override
            public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
            {
            }

            @Override
            public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
            {
                return null;
            }

            @Override
            public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
            {
                return null;
            }

            @Override
            public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, PrestoPrincipal principal)
            {
            }

            @Override
            public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
            {
            }

            @Override
            public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
            {
            }

            @Override
            public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, PrestoPrincipal principal)
            {
            }

            @Override
            public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
            {
            }

            @Override
            public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
            {
                return null;
            }

            @Override
            public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
            {
                return null;
            }

            @Override
            public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
            {
                return null;
            }

            @Override
            public void createMaterializedView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
            {
            }

            @Override
            public void dropMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
            {
            }

            @Override
            public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
            {
                return null;
            }

            @Override
            public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
            {
            }

            @Override
            public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
            {
                return null;
            }
        };
    }

    @Test
    public void testUpdateTableComment()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        abstractTrinoCatalogUnderTest.updateTableComment(session, schemaTableName, Optional.of("value"));

        // Verify the results
    }

    @Test
    public void testUpdateColumnComment()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        final ColumnIdentity columnIdentity = new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE,
                Arrays.asList());

        // Run the test
        abstractTrinoCatalogUnderTest.updateColumnComment(session, schemaTableName, columnIdentity,
                Optional.of("value"));

        // Verify the results
    }

    @Test
    public void testGetViews()
    {
        // Setup
        final ConnectorSession session = null;

        // Run the test
        final Map<SchemaTableName, ConnectorViewDefinition> result = abstractTrinoCatalogUnderTest.getViews(session,
                Optional.of("value"));

        // Verify the results
    }

    @Test
    public void testNewCreateTableTransaction()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        final Schema schema = new Schema(0, Arrays.asList(Types.NestedField.optional(0, "name", null)), new HashMap<>(),
                new HashSet<>(
                        Arrays.asList(0)));
        final PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
        final Map<String, String> properties = new HashMap<>();
        when(mockTableOperationsProvider.createTableOperations(any(TrinoCatalog.class), any(ConnectorSession.class),
                eq("schemaName"), eq("tableName"), eq(Optional.of("value")), eq(Optional.of("value"))))
                .thenReturn(null);

        // Run the test
        final Transaction result = abstractTrinoCatalogUnderTest.newCreateTableTransaction(session, schemaTableName,
                schema, partitionSpec, "location", properties, Optional.of("value"));

        // Verify the results
    }

    @Test
    public void testCreateNewTableName()
    {
        assertEquals("baseTableName", abstractTrinoCatalogUnderTest.createNewTableName("baseTableName"));
    }

    @Test
    public void testDeleteTableDirectory()
    {
        // Setup
        final ConnectorSession session = null;
        final SchemaTableName schemaTableName = new SchemaTableName("schemaName", "tableName");
        final HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setTargetMaxFileSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxInitialSplits(0);
        hiveConfig.setMaxInitialSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setSplitLoaderConcurrency(0);
        hiveConfig.setMaxSplitsPerSecond(0);
        hiveConfig.setDomainCompactionThreshold(0);
        hiveConfig.setWriterSortBufferSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setForceLocalScheduling(false);
        hiveConfig.setMaxConcurrentFileRenames(0);
        hiveConfig.setRecursiveDirWalkerEnabled(false);
        hiveConfig.setMaxSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxPartitionsPerScan(0);
        hiveConfig.setMaxOutstandingSplits(0);
        hiveConfig.setMaxOutstandingSplitsSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxSplitIteratorThreads(0);
        final HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(null, hiveConfig, null);
        final Path tableLocation = new Path("scheme", "authority", "path");

        // Run the test
        abstractTrinoCatalogUnderTest.deleteTableDirectory(session, schemaTableName, hdfsEnvironment, tableLocation);

        // Verify the results
    }

    @Test
    public void testGetView()
    {
        // Setup
        final SchemaTableName viewName = new SchemaTableName("schemaName", "tableName");
        final Map<String, String> tableParameters = new HashMap<>();

        // Run the test
        final Optional<ConnectorViewDefinition> result = abstractTrinoCatalogUnderTest.getView(viewName,
                Optional.of("value"), "tableType", tableParameters, Optional.of("value"));

        // Verify the results
    }

    @Test
    public void testCreateViewProperties()
    {
        // Setup
        final ConnectorSession session = null;
        final Map<String, String> expectedResult = new HashMap<>();

        // Run the test
        final Map<String, String> result = abstractTrinoCatalogUnderTest.createViewProperties(session);

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
