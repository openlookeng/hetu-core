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

import io.hetu.core.plugin.iceberg.ColumnIdentity;
import io.hetu.core.plugin.iceberg.IcebergColumnHandle;
import io.prestosql.spi.type.BooleanType;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TrinoDeleteFilterTest
{
    @Mock
    private FileScanTask mockTask;
    @Mock
    private Schema mockTableSchema;
    @Mock
    private FileIO mockFileIO;

    private TrinoDeleteFilter trinoDeleteFilterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        trinoDeleteFilterUnderTest = new TrinoDeleteFilter(mockTask, mockTableSchema,
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), BooleanType.BOOLEAN,
                        Arrays.asList(0), BooleanType.BOOLEAN, Optional.of("value"))), mockFileIO);
    }

    @Test
    public void testAsStructLike()
    {
        // Setup
        final TrinoRow row = null;

        // Run the test
        final StructLike result = trinoDeleteFilterUnderTest.asStructLike(row);

        // Verify the results
    }

    @Test
    public void testGetInputFile()
    {
        // Setup
        when(mockFileIO.newInputFile("s")).thenReturn(null);

        // Run the test
        final InputFile result = trinoDeleteFilterUnderTest.getInputFile("s");

        // Verify the results
    }
}
