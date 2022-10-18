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
package io.prestosql.spi.procedure;

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class ProcedureTest
{
    @Mock
    private MethodHandle mockMethodHandle;

    private Procedure procedureUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        procedureUnderTest = new Procedure("schema", "name",
                Arrays.asList(new Procedure.Argument("name", null, false, "defaultValue")), mockMethodHandle);
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = procedureUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }
}
