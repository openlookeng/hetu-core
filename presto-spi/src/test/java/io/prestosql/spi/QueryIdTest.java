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
package io.prestosql.spi;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class QueryIdTest
{
    private QueryId queryIdUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        queryIdUnderTest = new QueryId("id");
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("id", queryIdUnderTest.toString());
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, queryIdUnderTest.hashCode());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(queryIdUnderTest.equals("obj"));
    }

    @Test
    public void testValueOf() throws Exception
    {
        // Run the test
        final QueryId result = QueryId.valueOf("queryId");
        assertEquals("id", result.getId());
        assertEquals("id", result.toString());
        assertEquals(0, result.hashCode());
        assertTrue(result.equals("obj"));
    }

    @Test
    public void testValidateId() throws Exception
    {
        assertEquals("id", QueryId.validateId("id"));
    }

    @Test
    public void testParseDottedId() throws Exception
    {
        assertEquals(Arrays.asList("value"), QueryId.parseDottedId("id", 0, "name"));
        assertEquals(Collections.emptyList(), QueryId.parseDottedId("id", 0, "name"));
    }
}
