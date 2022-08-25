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
package io.prestosql.spi.relation;

//import io.prestosql.spi.block.TestMethodHandleUtil;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.type.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class CallExpressionTest
{
    @Mock
    private FunctionHandle mockFunctionHandle;
    @Mock
    private Type mockReturnType;

    private CallExpression callExpressionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        callExpressionUnderTest = new CallExpression("displayName", mockFunctionHandle, mockReturnType,
                Arrays.asList(), Optional.empty());
    }

    @Test
    public void testGetType() throws Exception
    {
        // Setup
        // Run the test
        final Type result = callExpressionUnderTest.getType();

        // Verify the results
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", callExpressionUnderTest.toString());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(callExpressionUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, callExpressionUnderTest.hashCode());
    }

//    @Test
//    public void testAccept() throws Exception
//    {
//        // Setup
//        final RowExpressionVisitor<R, C> visitor = null;
//        final C context = null;
//
//        // Run the test
//        callExpressionUnderTest.accept(visitor, context);
//
//        // Verify the results
//    }

    @Test
    public void testAbsEquals() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = callExpressionUnderTest.absEquals("o");

        // Verify the results
        assertTrue(result);
    }
}
