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

import io.prestosql.spi.connector.SchemaTableName;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.MockitoAnnotations.initMocks;

public class IcebergTableExecuteHandleTest
{
    @Mock
    private SchemaTableName mockSchemaTableName;
    @Mock
    private IcebergProcedureHandle mockProcedureHandle;

    private IcebergTableExecuteHandle icebergTableExecuteHandleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        icebergTableExecuteHandleUnderTest = new IcebergTableExecuteHandle(mockSchemaTableName,
                IcebergTableProcedureId.OPTIMIZE, mockProcedureHandle, "tableLocation");
    }

    @Test
    public void test()
    {
        icebergTableExecuteHandleUnderTest.getSchemaTableName();
        icebergTableExecuteHandleUnderTest.getProcedureId();
        icebergTableExecuteHandleUnderTest.getProcedureHandle();
        icebergTableExecuteHandleUnderTest.getTableLocation();
    }

    @Test
    public void testWithProcedureHandle()
    {
        // Setup
        final IcebergProcedureHandle procedureHandle = null;

        // Run the test
        final IcebergTableExecuteHandle result = icebergTableExecuteHandleUnderTest.withProcedureHandle(
                procedureHandle);

        // Verify the results
    }
}
