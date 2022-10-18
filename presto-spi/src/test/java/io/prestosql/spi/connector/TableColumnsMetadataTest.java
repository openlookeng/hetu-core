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
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class TableColumnsMetadataTest
{
    @Mock
    private SchemaTableName mockTable;

    private TableColumnsMetadata tableColumnsMetadataUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        tableColumnsMetadataUnderTest = new TableColumnsMetadata(mockTable, Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))));
    }

    @Test
    public void testForTable() throws Exception
    {
        // Setup
        final SchemaTableName table = new SchemaTableName("schemaName", "tableName");
        final List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false));

        // Run the test
        final TableColumnsMetadata result = TableColumnsMetadata.forTable(table, columns);
        assertEquals(new SchemaTableName("schemaName", "tableName"), result.getTable());
        assertEquals(Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), result.getColumns());
    }

    @Test
    public void testForRedirectedTable() throws Exception
    {
        // Setup
        final SchemaTableName table = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final TableColumnsMetadata result = TableColumnsMetadata.forRedirectedTable(table);
        assertEquals(new SchemaTableName("schemaName", "tableName"), result.getTable());
        assertEquals(Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), result.getColumns());
    }
}
