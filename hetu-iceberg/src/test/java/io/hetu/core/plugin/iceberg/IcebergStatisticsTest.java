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

import io.hetu.core.plugin.iceberg.delete.DummyFileScanTask;
import io.hetu.core.plugin.iceberg.delete.TrinoDeleteFile;
import io.prestosql.metadata.FunctionAndTypeManager;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;

public class IcebergStatisticsTest
{
    private IcebergStatistics icebergStatisticsUnderTest;

    private IcebergStatistics.Builder builder;

    @BeforeMethod
    public void setUp()
    {
        Types.NestedField neme = Types.NestedField.of(1, true, "neme", new Types.StringType());
        builder = new IcebergStatistics.Builder(Arrays.asList(neme), FunctionAndTypeManager.createTestFunctionAndTypeManager());

        icebergStatisticsUnderTest = builder.build();
    }

    @Test
    public void testAcceptDataFile()
    {
        HashMap<Integer, Long> integerLongHashMap = new HashMap<>();
        HashMap<Integer, byte[]> integerHashMap = new HashMap<>();
        Integer i = 2;
        TrinoDeleteFile path = new TrinoDeleteFile(i.longValue(), 1, FileContent.DATA, "path", FileFormat.ORC, 1, 1, integerLongHashMap, integerLongHashMap, integerLongHashMap, integerLongHashMap, integerHashMap, integerHashMap, "path".getBytes(StandardCharsets.UTF_8), Arrays.asList(1), 1, Arrays.asList(i.longValue()));
        DummyFileScanTask dummyFileScanTask = new DummyFileScanTask("path", Arrays.asList(path));
        DataFile file = dummyFileScanTask.file();
//        TrinoDeleteFile trinoDeleteFile = TrinoDeleteFile.copyOf(new TrinoDeleteFileTest.DeleteFile());
        builder.acceptDataFile(file, PartitionSpec.unpartitioned());
    }

    @Test
    public void test()
    {
        icebergStatisticsUnderTest.getRecordCount();
        icebergStatisticsUnderTest.getFileCount();
        icebergStatisticsUnderTest.getSize();
        icebergStatisticsUnderTest.getMinValues();
        icebergStatisticsUnderTest.getMaxValues();
        icebergStatisticsUnderTest.getNullCounts();
        icebergStatisticsUnderTest.getNullCounts();
        icebergStatisticsUnderTest.getNanCounts();
        icebergStatisticsUnderTest.getColumnSizes();
    }
}
