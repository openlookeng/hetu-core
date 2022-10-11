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

import io.prestosql.spi.connector.TableProcedureMetadata;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;

public class OptimizeTableProcedureTest
{
    private OptimizeTableProcedure optimizeTableProcedureUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        optimizeTableProcedureUnderTest = new OptimizeTableProcedure();
    }

    @Test
    public void testGet()
    {
        // Setup
        // Run the test
        final TableProcedureMetadata result = optimizeTableProcedureUnderTest.get();

        // Verify the results
    }

    @Test
    public void testGet_ThrowsRuntimeException()
    {
        // Setup
        // Run the test
        assertThrows(RuntimeException.class, () -> optimizeTableProcedureUnderTest.get());
    }
}
