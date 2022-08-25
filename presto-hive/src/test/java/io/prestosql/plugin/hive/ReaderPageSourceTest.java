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
package io.prestosql.plugin.hive;

import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static org.mockito.MockitoAnnotations.initMocks;

public class ReaderPageSourceTest
{
    @Mock
    private ConnectorPageSource mockConnectorPageSource;

    private ReaderPageSource readerPageSourceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        HiveColumnHandle hiveColumnHandle = new HiveColumnHandle("name", HIVE_STRING, new TypeSignature("base", TypeSignatureParameter.of(0L)), 0, HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false);
        List<HiveColumnHandle> hiveColumnHandles = Arrays.asList(hiveColumnHandle);
        readerPageSourceUnderTest = new ReaderPageSource(mockConnectorPageSource, Optional.of(new ReaderColumns(hiveColumnHandles, Arrays.asList(0))));
    }

    @Test
    public void testGet() throws Exception
    {
        // Setup
        // Run the test
        final ConnectorPageSource result = readerPageSourceUnderTest.get();

        // Verify the results
    }

    @Test
    public void testGetReaderColumns()
    {
        // Setup
        // Run the test
        final Optional<ReaderColumns> result = readerPageSourceUnderTest.getReaderColumns();

        // Verify the results
    }

    @Test
    public void testNoProjectionAdaptation()
    {
        // Setup
        final ConnectorPageSource connectorPageSource = null;

        // Run the test
        final ReaderPageSource result = ReaderPageSource.noProjectionAdaptation(connectorPageSource);
    }
}
