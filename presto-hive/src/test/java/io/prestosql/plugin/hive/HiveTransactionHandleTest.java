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

import java.util.UUID;

public class HiveTransactionHandleTest
{
    private HiveTransactionHandle hiveTransactionHandleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        hiveTransactionHandleUnderTest = new HiveTransactionHandle(false,
                UUID.fromString("ae4c83a5-92cf-4e3a-adf7-874e454f226d"));
    }

    @Test
    public void testHiveTransactionHandle()
    {
        HiveTransactionHandle hiveTransactionHandle = new HiveTransactionHandle();
    }

    @Test
    public void testGetUuid()
    {
        UUID uuid = hiveTransactionHandleUnderTest.getUuid();
    }

    @Test
    public void testIsAutoCommit()
    {
        boolean autoCommit = hiveTransactionHandleUnderTest.isAutoCommit();
    }

    @Test
    public void testEquals() throws Exception
    {
        hiveTransactionHandleUnderTest.equals("obj");
    }

    @Test
    public void testHashCode() throws Exception
    {
        hiveTransactionHandleUnderTest.hashCode();
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = hiveTransactionHandleUnderTest.toString();
    }
}
