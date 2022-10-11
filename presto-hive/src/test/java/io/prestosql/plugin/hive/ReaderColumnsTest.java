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

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;

public class ReaderColumnsTest
{
    private ReaderColumns readerColumnsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        HiveColumnHandle hiveColumnHandle = new HiveColumnHandle("name", HIVE_STRING, new TypeSignature("base", TypeSignatureParameter.of(0L)), 0, HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false);
        List<HiveColumnHandle> hiveColumnHandles = Arrays.asList(hiveColumnHandle);
        readerColumnsUnderTest = new ReaderColumns(hiveColumnHandles, Arrays.asList(0));
    }

    @Test
    public void testGetForColumnAt()
    {
        // Setup
        // Run the test
        final ColumnHandle result = readerColumnsUnderTest.getForColumnAt(0);

        // Verify the results
    }

    @Test
    public void testGetPositionForColumnAt()
    {
        // Setup
        // Run the test
        final int result = readerColumnsUnderTest.getPositionForColumnAt(0);
    }

    @Test
    public void testGet() throws Exception
    {
        // Setup
        // Run the test
        final List<ColumnHandle> result = readerColumnsUnderTest.get();

        // Verify the results
    }
}
