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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class IcebergHandleResolverTest
{
    private IcebergHandleResolver icebergHandleResolverUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        icebergHandleResolverUnderTest = new IcebergHandleResolver();
    }

    @Test
    public void testGetTableHandleClass()
    {
        assertEquals(Object.class, icebergHandleResolverUnderTest.getTableHandleClass());
    }

    @Test
    public void testGetTableLayoutHandleClass()
    {
        assertEquals(Object.class, icebergHandleResolverUnderTest.getTableLayoutHandleClass());
    }

    @Test
    public void testGetColumnHandleClass()
    {
        assertEquals(Object.class, icebergHandleResolverUnderTest.getColumnHandleClass());
    }

    @Test
    public void testGetSplitClass()
    {
        assertEquals(Object.class, icebergHandleResolverUnderTest.getSplitClass());
    }

    @Test
    public void testGetOutputTableHandleClass()
    {
        assertEquals(Object.class, icebergHandleResolverUnderTest.getOutputTableHandleClass());
    }

    @Test
    public void testGetInsertTableHandleClass()
    {
        assertEquals(Object.class, icebergHandleResolverUnderTest.getInsertTableHandleClass());
    }

    @Test
    public void testGetTransactionHandleClass()
    {
        assertEquals(Object.class, icebergHandleResolverUnderTest.getTransactionHandleClass());
    }
}
