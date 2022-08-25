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
package io.prestosql.spi.connector;

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ConnectorTableSchemaTest
{
    @Mock
    private SchemaTableName mockTable;

    private ConnectorTableSchema connectorTableSchemaUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        connectorTableSchemaUnderTest = new ConnectorTableSchema(mockTable, Arrays.asList());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", connectorTableSchemaUnderTest.toString());
    }
}
