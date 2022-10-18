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

import io.prestosql.spi.predicate.TupleDomain;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class IcebergTableLayoutHandleTest
{
    @Mock
    private IcebergTableHandle mockTable;

    private IcebergTableLayoutHandle icebergTableLayoutHandleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        icebergTableLayoutHandleUnderTest = new IcebergTableLayoutHandle(mockTable,
                TupleDomain.withColumnDomains(new HashMap<>()));
    }

    @Test
    public void testGetTable()
    {
        icebergTableLayoutHandleUnderTest.getTable();
    }

    @Test
    public void testGetTupleDomain()
    {
        icebergTableLayoutHandleUnderTest.getTupleDomain();
    }

    @Test
    public void testEquals()
    {
        assertTrue(icebergTableLayoutHandleUnderTest.equals("o"));
    }

    @Test
    public void testHashCode()
    {
        assertEquals(0, icebergTableLayoutHandleUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        when(mockTable.toString()).thenReturn("result");

        // Run the test
        final String result = icebergTableLayoutHandleUnderTest.toString();

        // Verify the results
        assertEquals("table", result);
    }
}
