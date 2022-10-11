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
import io.prestosql.plugin.hive.HiveTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.apache.iceberg.Table;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class FilesTableTest
{
    @Mock
    private SchemaTableName mockTableName;
    @Mock
    private TypeManager mockTypeManager;
    @Mock
    private Table mockIcebergTable;

    private FilesTable filesTableUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        filesTableUnderTest = new FilesTable(mockTableName, FunctionAndTypeManager.createTestFunctionAndTypeManager(), mockIcebergTable, Optional.of(0L));
    }

    @Test
    public void testGetDistribution()
    {
        filesTableUnderTest.getDistribution();
        filesTableUnderTest.getTableMetadata();
    }

    @Test
    public void testPageSource()
    {
        // Setup
        final TupleDomain<Integer> constraint = TupleDomain.withColumnDomains(new HashMap<>());

        // Run the test
        filesTableUnderTest.pageSource(new HiveTransactionHandle(), new HdfsFileIoProviderTest.ConnectorSession(), constraint);

        // Verify the results
    }
}
