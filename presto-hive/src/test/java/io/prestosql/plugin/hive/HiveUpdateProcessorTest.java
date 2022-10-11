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

import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.ColumnarRow;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class HiveUpdateProcessorTest
{
    private HiveUpdateProcessor hiveUpdateProcessorUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        hiveUpdateProcessorUnderTest = new HiveUpdateProcessor(
                Arrays.asList(new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false)),
                Arrays.asList(new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false)));
    }

    @Test
    public void testMergeWithNonUpdatedColumns()
    {
        // Setup
        final List<HiveColumnHandle> updateDependencies = Arrays.asList(
                new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false));
        final List<HiveColumnHandle> expectedResult = Arrays.asList(
                new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false));

        // Run the test
        final List<HiveColumnHandle> result = hiveUpdateProcessorUnderTest.mergeWithNonUpdatedColumns(
                updateDependencies);
    }

    @Test
    public void testRemoveNonDependencyColumns()
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);

        // Run the test
        final Page result = hiveUpdateProcessorUnderTest.removeNonDependencyColumns(page, Arrays.asList(0));

        // Verify the results
    }

    @Test
    public void testGetAcidBlock()
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);

        // Run the test
        final ColumnarRow result = hiveUpdateProcessorUnderTest.getAcidBlock(page, Arrays.asList(0));

        // Verify the results
    }

    @Test
    public void testCreateMergedColumnsBlock()
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);

        // Run the test
        final Block result = hiveUpdateProcessorUnderTest.createMergedColumnsBlock(page, Arrays.asList(0));

        // Verify the results
    }

    @Test
    public void testMakeDependencyChannelNumbers()
    {
        // Setup
        final List<HiveColumnHandle> dependencyColumns = Arrays.asList(
                new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false));

        // Run the test
        final List<Integer> result = hiveUpdateProcessorUnderTest.makeDependencyChannelNumbers(dependencyColumns);
    }

    @Test
    public void testMakeNonUpdatedSourceChannels()
    {
        // Setup
        final List<HiveColumnHandle> dependencyColumns = Arrays.asList(
                new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false));

        // Run the test
        final List<Integer> result = hiveUpdateProcessorUnderTest.makeNonUpdatedSourceChannels(dependencyColumns);
    }
}
