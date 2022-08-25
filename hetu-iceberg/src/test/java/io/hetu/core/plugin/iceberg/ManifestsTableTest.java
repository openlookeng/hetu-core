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
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.iceberg.Table;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ManifestsTableTest
{
    @Mock
    private SchemaTableName mockTableName;
    @Mock
    private Table mockIcebergTable;

    private ManifestsTable manifestsTableUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        manifestsTableUnderTest = new ManifestsTable(mockTableName, mockIcebergTable, Optional.of(0L));
    }

    @Test
    public void testGetDistribution()
    {
        assertEquals(SystemTable.Distribution.ALL_NODES, manifestsTableUnderTest.getDistribution());
    }

    @Test
    public void testPageSource()
    {
        // Setup
        final TupleDomain<Integer> constraint = TupleDomain.withColumnDomains(new HashMap<>());

        // Run the test
        final ConnectorPageSource result = manifestsTableUnderTest.pageSource(null, null, constraint);

        // Verify the results
    }

    @Test
    public void test()
    {
        manifestsTableUnderTest.getTableMetadata();
    }
}
