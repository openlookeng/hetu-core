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
package io.hetu.core.plugin.iceberg.procedure;

import io.airlift.units.DataSize;
import io.hetu.core.plugin.iceberg.ColumnIdentity;
import io.hetu.core.plugin.iceberg.IcebergColumnHandle;
import io.hetu.core.plugin.iceberg.IcebergFileFormat;
import io.prestosql.spi.type.BooleanType;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class IcebergOptimizeHandleTest
{
    @Mock
    private DataSize mockMaxScannedFileSize;

    private IcebergOptimizeHandle icebergOptimizeHandleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        icebergOptimizeHandleUnderTest = new IcebergOptimizeHandle("schemaAsJson", "partitionSpecAsJson",
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), BooleanType.BOOLEAN,
                        Arrays.asList(0), BooleanType.BOOLEAN, Optional.of("value"))),
                IcebergFileFormat.ORC, new HashMap<>(), mockMaxScannedFileSize, false);
    }

    @Test
    public void test()
    {
        icebergOptimizeHandleUnderTest.getSchemaAsJson();
        icebergOptimizeHandleUnderTest.getPartitionSpecAsJson();
        icebergOptimizeHandleUnderTest.getTableColumns();
        icebergOptimizeHandleUnderTest.getFileFormat();
        icebergOptimizeHandleUnderTest.getTableStorageProperties();
        icebergOptimizeHandleUnderTest.getMaxScannedFileSize();
        icebergOptimizeHandleUnderTest.isRetriesEnabled();
    }

    @Test
    public void testToString()
    {
        assertEquals("result", icebergOptimizeHandleUnderTest.toString());
    }
}
