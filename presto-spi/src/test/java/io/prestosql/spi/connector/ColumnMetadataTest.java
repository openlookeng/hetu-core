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
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ColumnMetadataTest
{
    @Mock
    private Type mockType;

    private ColumnMetadata columnMetadataUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        columnMetadataUnderTest = new ColumnMetadata("name", mockType, false, "comment", "extraInfo", false,
                new HashMap<>(), false);
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = columnMetadataUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, columnMetadataUnderTest.hashCode());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(columnMetadataUnderTest.equals("obj"));
    }

    @Test
    public void testBuilder() throws Exception
    {
        // Setup
        // Run the test
        final ColumnMetadata.Builder result = ColumnMetadata.builder();

        // Verify the results
    }
}
