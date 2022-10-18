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

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class IcebergTableNameTest
{
    private IcebergTableName icebergTableNameUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        icebergTableNameUnderTest = new IcebergTableName("tableName", TableType.DATA, Optional.of(0L));
    }

    @Test
    public void testGetTableNameWithType()
    {
        // Setup
        // Run the test
        final String result = icebergTableNameUnderTest.getTableNameWithType();
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = icebergTableNameUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void test()
    {
        // Run the test
        final IcebergTableName result = IcebergTableName.from("name");
        result.getTableName();
        result.getTableType();
        result.getSnapshotId();
        result.getTableNameWithType();
        result.toString();
    }

    @Test
    public void testFrom()
    {
        // Run the test
        IcebergTableName.from("name");
    }
}
