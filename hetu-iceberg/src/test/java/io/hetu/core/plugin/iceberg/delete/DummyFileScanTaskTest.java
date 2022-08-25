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
package io.hetu.core.plugin.iceberg.delete;

import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class DummyFileScanTaskTest
{
    private DummyFileScanTask dummyFileScanTaskUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        dummyFileScanTaskUnderTest = new DummyFileScanTask("path",
                Arrays.asList(
                        new TrinoDeleteFile(0L, 0, FileContent.DATA, "path", FileFormat.ORC, 0L, 0L, new HashMap<>(),
                                new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(),
                                "content".getBytes(), Arrays.asList(0), 0, Arrays.asList(0L))));
    }

    @Test
    public void test()
    {
        dummyFileScanTaskUnderTest.file();
        dummyFileScanTaskUnderTest.deletes();
    }

    @Test
    public void testSpec()
    {
        // Setup
        final PartitionSpec expectedResult = PartitionSpec.unpartitioned();

        // Run the test
        final PartitionSpec result = dummyFileScanTaskUnderTest.spec();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testStart()
    {
        assertEquals(0L, dummyFileScanTaskUnderTest.start());
    }

    @Test
    public void testLength()
    {
        assertEquals(0L, dummyFileScanTaskUnderTest.length());
    }

    @Test
    public void testResidual()
    {
        // Setup
        // Run the test
        final Expression result = dummyFileScanTaskUnderTest.residual();

        // Verify the results
    }

    @Test
    public void testSplit()
    {
        // Setup
        // Run the test
        final Iterable<FileScanTask> result = dummyFileScanTaskUnderTest.split(0L);

        // Verify the results
    }

    @Test
    public void testIsFileScanTask()
    {
        assertTrue(dummyFileScanTaskUnderTest.isFileScanTask());
    }

    @Test
    public void testAsFileScanTask()
    {
        // Setup
        // Run the test
        final FileScanTask result = dummyFileScanTaskUnderTest.asFileScanTask();

        // Verify the results
    }

    @Test
    public void testIsDataTask()
    {
        assertTrue(dummyFileScanTaskUnderTest.isDataTask());
    }

    @Test
    public void testAsDataTask()
    {
        // Setup
        // Run the test
        final DataTask result = dummyFileScanTaskUnderTest.asDataTask();

        // Verify the results
    }

    @Test
    public void testAsDataTask_ThrowsIllegalStateException()
    {
        // Setup
        // Run the test
        assertThrows(IllegalStateException.class, () -> dummyFileScanTaskUnderTest.asDataTask());
    }

    @Test
    public void testAsCombinedScanTask()
    {
        // Setup
        // Run the test
        final CombinedScanTask result = dummyFileScanTaskUnderTest.asCombinedScanTask();

        // Verify the results
    }

    @Test
    public void testAsCombinedScanTask_ThrowsIllegalStateException()
    {
        // Setup
        // Run the test
        assertThrows(IllegalStateException.class, () -> dummyFileScanTaskUnderTest.asCombinedScanTask());
    }
}
