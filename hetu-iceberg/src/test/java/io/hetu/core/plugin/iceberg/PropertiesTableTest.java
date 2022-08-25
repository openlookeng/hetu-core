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

import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
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

import static org.mockito.MockitoAnnotations.initMocks;

public class PropertiesTableTest
{
    @Mock
    private SchemaTableName mockTableName;
    @Mock
    private Table mockIcebergTable;

    private PropertiesTable propertiesTableUnderTest;

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
        propertiesTableUnderTest = new PropertiesTable(mockTableName, new BaseTable(tableOperations, "name"));
    }

    @Test
    public void testGetDistribution()
    {
        propertiesTableUnderTest.getDistribution();
    }

    @Test
    public void testGetTableMetadata()
    {
        propertiesTableUnderTest.getTableMetadata();
    }

    @Test
    public void testPageSource()
    {
        // Setup
        final TupleDomain<Integer> constraint = TupleDomain.withColumnDomains(new HashMap<>());

        // Run the test
        final ConnectorPageSource result = propertiesTableUnderTest.pageSource(null, null, constraint);

        // Verify the results
    }
}
