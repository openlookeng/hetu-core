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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HiveHandleResolverTest
{
    private HiveHandleResolver hiveHandleResolverUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        hiveHandleResolverUnderTest = new HiveHandleResolver();
    }

    @Test
    public void testGetTableHandleClass()
    {
        hiveHandleResolverUnderTest.getTableHandleClass();
    }

    @Test
    public void testGetColumnHandleClass()
    {
        hiveHandleResolverUnderTest.getColumnHandleClass();
    }

    @Test
    public void testGetSplitClass()
    {
        hiveHandleResolverUnderTest.getSplitClass();
    }

    @Test
    public void testGetOutputTableHandleClass()
    {
        hiveHandleResolverUnderTest.getOutputTableHandleClass();
    }

    @Test
    public void testGetInsertTableHandleClass()
    {
        hiveHandleResolverUnderTest.getInsertTableHandleClass();
    }

    @Test
    public void testGetUpdateTableHandleClass()
    {
        hiveHandleResolverUnderTest.getUpdateTableHandleClass();
    }

    @Test
    public void testGetDeleteAsInsertTableHandleClass()
    {
        hiveHandleResolverUnderTest.getDeleteAsInsertTableHandleClass();
    }

    @Test
    public void testGetVacuumTableHandleClass()
    {
        hiveHandleResolverUnderTest.getVacuumTableHandleClass();
    }

    @Test
    public void testGetTransactionHandleClass()
    {
        hiveHandleResolverUnderTest.getTransactionHandleClass();
    }

    @Test
    public void testGetPartitioningHandleClass()
    {
        hiveHandleResolverUnderTest.getPartitioningHandleClass();
    }
}
