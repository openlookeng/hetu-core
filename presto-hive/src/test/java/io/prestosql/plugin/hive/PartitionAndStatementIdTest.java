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

import java.util.List;
import java.util.Optional;

public class PartitionAndStatementIdTest
{
    private PartitionAndStatementId partitionAndStatementIdUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        partitionAndStatementIdUnderTest = new PartitionAndStatementId("partitionName", 0, 0L, "deleteDeltaDirectory",
                Optional.of("value"));
    }

    @Test
    public void testGetAllDirectories()
    {
        // Setup
        // Run the test
        final List<String> result = partitionAndStatementIdUnderTest.getAllDirectories();
    }

    @Test
    public void testEquals() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = partitionAndStatementIdUnderTest.equals("o");
    }

    @Test
    public void testHashCode()
    {
        partitionAndStatementIdUnderTest.hashCode();
    }
}
