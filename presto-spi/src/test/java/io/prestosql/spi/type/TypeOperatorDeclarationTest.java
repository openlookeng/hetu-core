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
package io.prestosql.spi.type;

import io.prestosql.spi.function.IcebergInvocationConvention;
import io.prestosql.spi.function.OperatorMethodHandle;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TypeOperatorDeclarationTest
{
    @Test
    public void testBuilder() throws Exception
    {
        // Setup
        // Run the test
        final TypeOperatorDeclaration.Builder result = TypeOperatorDeclaration.builder(Object.class);

        // Verify the results
    }

    @Test
    public void testExtractOperatorDeclaration() throws Exception
    {
        // Setup
        final MethodHandles.Lookup lookup = null;

        // Run the test
        final TypeOperatorDeclaration result = TypeOperatorDeclaration.extractOperatorDeclaration(Object.class, lookup,
                Object.class);
        assertTrue(result.isComparable());
        assertTrue(result.isOrderable());
        assertEquals(Arrays.asList(new OperatorMethodHandle(new IcebergInvocationConvention(
                        Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                        IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false), null)),
                result.getEqualOperators());
        assertEquals(Arrays.asList(new OperatorMethodHandle(new IcebergInvocationConvention(
                        Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                        IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false), null)),
                result.getHashCodeOperators());
        assertEquals(Arrays.asList(new OperatorMethodHandle(new IcebergInvocationConvention(
                        Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                        IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false), null)),
                result.getXxHash64Operators());
        assertEquals(Arrays.asList(new OperatorMethodHandle(new IcebergInvocationConvention(
                        Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                        IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false), null)),
                result.getDistinctFromOperators());
        assertEquals(Arrays.asList(new OperatorMethodHandle(new IcebergInvocationConvention(
                        Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                        IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false), null)),
                result.getIndeterminateOperators());
        assertEquals(Arrays.asList(new OperatorMethodHandle(new IcebergInvocationConvention(
                        Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                        IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false), null)),
                result.getComparisonUnorderedLastOperators());
        assertEquals(Arrays.asList(new OperatorMethodHandle(new IcebergInvocationConvention(
                        Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                        IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false), null)),
                result.getComparisonUnorderedFirstOperators());
        assertEquals(Arrays.asList(new OperatorMethodHandle(new IcebergInvocationConvention(
                        Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                        IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false), null)),
                result.getLessThanOperators());
        assertEquals(Arrays.asList(new OperatorMethodHandle(new IcebergInvocationConvention(
                        Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                        IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false), null)),
                result.getLessThanOrEqualOperators());
    }
}
