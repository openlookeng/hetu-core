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

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.type.TimeZoneKey;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class HistoryTableTest
{
    @Mock
    private SchemaTableName mockTableName;
    @Mock
    private Table mockIcebergTable;

    private HistoryTable historyTableUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        BaseTable baseTable = new BaseTable(new TableOperations(), "name");
        historyTableUnderTest = new HistoryTable(mockTableName, baseTable);
    }

    public static class TableOperations
            implements org.apache.iceberg.TableOperations
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
    }

    @Test
    public void testGetDistribution()
    {
        assertEquals(SystemTable.Distribution.ALL_NODES, historyTableUnderTest.getDistribution());
    }

    @Test
    public void testGetTableMetadata()
    {
        historyTableUnderTest.getTableMetadata();
    }

    @Test
    public void testCursor()
    {
        // Setup
        ConnectorSession connectorSession = new ConnectorSession()
        {
            @Override
            public String getQueryId()
            {
                return "queryId";
            }

            @Override
            public Optional<String> getSource()
            {
                return Optional.empty();
            }

            @Override
            public ConnectorIdentity getIdentity()
            {
                return new ConnectorIdentity("str", Optional.empty(), Optional.empty());
            }

            @Override
            public TimeZoneKey getTimeZoneKey()
            {
                return null;
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
        };
        final TupleDomain<Integer> constraint = TupleDomain.withColumnDomains(new HashMap<>());
        when(mockIcebergTable.history()).thenReturn(Arrays.asList());
        when(mockIcebergTable.snapshot(0L)).thenReturn(null);

        // Run the test
        final RecordCursor result = historyTableUnderTest.cursor(null, connectorSession, constraint);

        // Verify the results
    }
}
