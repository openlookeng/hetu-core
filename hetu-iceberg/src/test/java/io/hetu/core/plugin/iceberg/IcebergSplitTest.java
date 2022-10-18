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

import io.hetu.core.plugin.iceberg.delete.TrinoDeleteFile;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.SplitWeight;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;

import static org.testng.Assert.assertTrue;

public class IcebergSplitTest
{
    @Mock
    private SplitWeight splitWeight;
    private IcebergSplit icebergSplitUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        icebergSplitUnderTest = new IcebergSplit(
                "path",
                1,
                1,
                1,
                1,
                IcebergFileFormat.ORC,
                Arrays.asList(new HostAddress("host", 0)),
                "partitionSpecJson",
                "partitionDataJson",
                Arrays.asList(new TrinoDeleteFile(0L, 0, FileContent.DATA, "path", FileFormat.ORC, 0L, 0L, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), "content".getBytes(StandardCharsets.UTF_8), Arrays.asList(0), 0, Arrays.asList(0L))),
                SplitWeight.standard());
    }

    @Test
    public void testIsRemotelyAccessible()
    {
        assertTrue(icebergSplitUnderTest.isRemotelyAccessible());
    }

    @Test
    public void test()
    {
        icebergSplitUnderTest.getAddresses();
        icebergSplitUnderTest.getPath();
        icebergSplitUnderTest.getStart();
        icebergSplitUnderTest.getLength();
        icebergSplitUnderTest.getFileSize();
        icebergSplitUnderTest.getFileRecordCount();
        icebergSplitUnderTest.getFileFormat();
        icebergSplitUnderTest.getPartitionSpecJson();
        icebergSplitUnderTest.getPartitionDataJson();
        icebergSplitUnderTest.getDeletes();
        icebergSplitUnderTest.getSplitWeight();
    }

    @Test
    public void testGetInfo()
    {
        icebergSplitUnderTest.getInfo();
    }

    @Test
    public void testGetRetainedSizeInBytes()
    {
        // Setup
        // Run the test
        final long result = icebergSplitUnderTest.getRetainedSizeInBytes();
    }

    @Test
    public void testToString()
    {
        icebergSplitUnderTest.toString();
    }
}
