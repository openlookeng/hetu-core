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

import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.function.IcebergInvocationConvention;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;

public class TypeOperatorsTest
{
    private TypeOperators typeOperatorsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        typeOperatorsUnderTest = new TypeOperators((val1, val2) -> {
            return "value";
        });
    }

    @Test
    public void testGetEqualOperator() throws Exception
    {
        // Setup
        final Type type = null;
        final IcebergInvocationConvention callingConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);

        // Run the test
        final MethodHandle result = typeOperatorsUnderTest.getEqualOperator(type, callingConvention);

        // Verify the results
    }

    @Test
    public void testGetHashCodeOperator() throws Exception
    {
        // Setup
        final Type type = null;
        final IcebergInvocationConvention callingConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);

        // Run the test
        final MethodHandle result = typeOperatorsUnderTest.getHashCodeOperator(type, callingConvention);

        // Verify the results
    }

    @Test
    public void testGetXxHash64Operator()
    {
        // Setup
        final Type type = null;
        final IcebergInvocationConvention callingConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);

        // Run the test
        final MethodHandle result = typeOperatorsUnderTest.getXxHash64Operator(type, callingConvention);

        // Verify the results
    }

    @Test
    public void testGetDistinctFromOperator() throws Exception
    {
        // Setup
        final Type type = null;
        final IcebergInvocationConvention callingConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);

        // Run the test
        final MethodHandle result = typeOperatorsUnderTest.getDistinctFromOperator(type, callingConvention);

        // Verify the results
    }

    @Test
    public void testGetIndeterminateOperator() throws Exception
    {
        // Setup
        final Type type = null;
        final IcebergInvocationConvention callingConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);

        // Run the test
        final MethodHandle result = typeOperatorsUnderTest.getIndeterminateOperator(type, callingConvention);

        // Verify the results
    }

    @Test
    public void testGetComparisonUnorderedLastOperator() throws Exception
    {
        // Setup
        final Type type = null;
        final IcebergInvocationConvention callingConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);

        // Run the test
        final MethodHandle result = typeOperatorsUnderTest.getComparisonUnorderedLastOperator(type, callingConvention);

        // Verify the results
    }

    @Test
    public void testGetComparisonUnorderedFirstOperator() throws Exception
    {
        // Setup
        final Type type = null;
        final IcebergInvocationConvention callingConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);

        // Run the test
        final MethodHandle result = typeOperatorsUnderTest.getComparisonUnorderedFirstOperator(type, callingConvention);

        // Verify the results
    }

    @Test
    public void testGetOrderingOperator() throws Exception
    {
        // Setup
        final Type type = null;
        final IcebergInvocationConvention callingConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);

        // Run the test
        final MethodHandle result = typeOperatorsUnderTest.getOrderingOperator(type, SortOrder.ASC_NULLS_FIRST,
                callingConvention);

        // Verify the results
    }

    @Test
    public void testGetLessThanOperator() throws Exception
    {
        // Setup
        final Type type = null;
        final IcebergInvocationConvention callingConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);

        // Run the test
        final MethodHandle result = typeOperatorsUnderTest.getLessThanOperator(type, callingConvention);

        // Verify the results
    }

    @Test
    public void testGetLessThanOrEqualOperator() throws Exception
    {
        // Setup
        final Type type = null;
        final IcebergInvocationConvention callingConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);

        // Run the test
        final MethodHandle result = typeOperatorsUnderTest.getLessThanOrEqualOperator(type, callingConvention);

        // Verify the results
    }
}
