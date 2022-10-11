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

import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TypeManager;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class SnapshotsTableTest
{
    @Mock
    private SchemaTableName mockTableName;
    @Mock
    private TypeManager mockTypeManager;
    @Mock
    private Table mockIcebergTable;
    private SnapshotsTable snapshotsTableUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        TableOperations tableOperations = new TableOperations()
        {
            @Override
            public TableMetadata current()
            {
                HashMap<String, String> stringStringHashMap = new HashMap<>();
                stringStringHashMap.put("key", "value");
                TableMetadata location = TableMetadata.newTableMetadata(new Schema(), PartitionSpec.unpartitioned(), SortOrder.unsorted(), "location", stringStringHashMap);
                Snapshot snapshot = new Snapshot()
                {
                    @Override
                    public long sequenceNumber()
                    {
                        return 0;
                    }

                    @Override
                    public long snapshotId()
                    {
                        return 0;
                    }

                    @Override
                    public Long parentId()
                    {
                        return null;
                    }

                    @Override
                    public long timestampMillis()
                    {
                        return 0;
                    }

                    @Override
                    public List<ManifestFile> allManifests()
                    {
                        return null;
                    }

                    @Override
                    public List<ManifestFile> dataManifests()
                    {
                        return null;
                    }

                    @Override
                    public List<ManifestFile> deleteManifests()
                    {
                        return null;
                    }

                    @Override
                    public String operation()
                    {
                        return null;
                    }

                    @Override
                    public Map<String, String> summary()
                    {
                        return null;
                    }

                    @Override
                    public Iterable<DataFile> addedFiles()
                    {
                        return null;
                    }

                    @Override
                    public Iterable<DataFile> deletedFiles()
                    {
                        return null;
                    }

                    @Override
                    public String manifestListLocation()
                    {
                        return null;
                    }
                };
                TableMetadata tableMetadata = location.replaceCurrentSnapshot(snapshot);
                return tableMetadata;
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

        BaseTable name = new BaseTable(tableOperations, "name");
        snapshotsTableUnderTest = new SnapshotsTable(mockTableName, FunctionAndTypeManager.createTestFunctionAndTypeManager(), name);
    }

    @Test
    public void testGetDistribution()
    {
        snapshotsTableUnderTest.getDistribution();
    }

    @Test
    public void testGetTableMetadata()
    {
        snapshotsTableUnderTest.getTableMetadata();
    }

    @Test
    public void testPageSource()
    {
        // Setup
        final ConnectorSession connectorSession = new ConnectorSession()
        {
            @Override
            public String getQueryId()
            {
                return null;
            }

            @Override
            public Optional<String> getSource()
            {
                return Optional.empty();
            }

            @Override
            public ConnectorIdentity getIdentity()
            {
                return null;
            }

            @Override
            public TimeZoneKey getTimeZoneKey()
            {
                return TimeZoneKey.UTC_KEY;
            }

            @Override
            public Locale getLocale()
            {
                return null;
            }

            @Override
            public Optional<String> getTraceToken()
            {
                return Optional.empty();
            }

            @Override
            public long getStartTime()
            {
                return 0;
            }

            @Override
            public <T> T getProperty(String name, Class<T> type)
            {
                return null;
            }

            @Override
            public Optional<String> getCatalog()
            {
                return Optional.of("tpch");
            }
        };
        // Run the test
        snapshotsTableUnderTest.pageSource(null, connectorSession, null);

        // Verify the results
    }
}
