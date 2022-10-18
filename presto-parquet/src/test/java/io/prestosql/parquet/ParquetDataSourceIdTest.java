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
package io.prestosql.parquet;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ParquetDataSourceIdTest
{
    private ParquetDataSourceId parquetDataSourceIdUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        parquetDataSourceIdUnderTest = new ParquetDataSourceId("id");
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(parquetDataSourceIdUnderTest.equals("o"));
    }

    @Test
    public void testHashCode()
    {
        assertEquals(0, parquetDataSourceIdUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("id", parquetDataSourceIdUnderTest.toString());
    }
}
