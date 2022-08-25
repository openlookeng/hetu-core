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

import io.prestosql.spi.type.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

public class InMemoryRecordSetTest
{
    private InMemoryRecordSet inMemoryRecordSetUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        inMemoryRecordSetUnderTest = new InMemoryRecordSet(Arrays.asList(), Arrays.asList(Arrays.asList()));
    }

    @Test
    public void testGetColumnTypes() throws Exception
    {
        // Setup
        // Run the test
        final List<Type> result = inMemoryRecordSetUnderTest.getColumnTypes();

        // Verify the results
    }

    @Test
    public void testCursor() throws Exception
    {
        // Setup
        // Run the test
        final RecordCursor result = inMemoryRecordSetUnderTest.cursor();

        // Verify the results
    }

    @Test
    public void testBuilder1() throws Exception
    {
        // Setup
        final ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("schemaName", "tableName"), Arrays.asList(
                        new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)),
                new HashMap<>(), Optional.of("value"), Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), Optional.of(new HashSet<>(
                Arrays.asList("value"))));

        // Run the test
        final InMemoryRecordSet.Builder result = InMemoryRecordSet.builder(tableMetadata);

        // Verify the results
    }

    @Test
    public void testBuilder2() throws Exception
    {
        // Setup
        final List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false));

        // Run the test
        final InMemoryRecordSet.Builder result = InMemoryRecordSet.builder(columns);

        // Verify the results
    }

    @Test
    public void testBuilder3() throws Exception
    {
        // Setup
        final Collection<Type> columnsTypes = Arrays.asList();

        // Run the test
        final InMemoryRecordSet.Builder result = InMemoryRecordSet.builder(columnsTypes);

        // Verify the results
    }
}
