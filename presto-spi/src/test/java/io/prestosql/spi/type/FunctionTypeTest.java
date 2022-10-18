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

import io.airlift.slice.Slice;
import io.prestosql.spi.block.BlockBuilder;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class FunctionTypeTest
{
    @Mock
    private Type mockReturnType;

    private FunctionType functionTypeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        functionTypeUnderTest = new FunctionType(Arrays.asList(), mockReturnType);
    }

    @Test
    public void testGetTypeParameters() throws Exception
    {
        // Setup
        // Run the test
        final List<Type> result = functionTypeUnderTest.getTypeParameters();

        // Verify the results
    }

    @Test
    public void testGetTypeSignature() throws Exception
    {
        // Setup
        final TypeSignature expectedResult = new TypeSignature("base", TypeSignatureParameter.of(0L));

        // Run the test
        final TypeSignature result = functionTypeUnderTest.getTypeSignature();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetDisplayName() throws Exception
    {
        assertEquals("result", functionTypeUnderTest.getDisplayName());
    }

    @Test
    public void testGetJavaType() throws Exception
    {
        assertEquals(Object.class, functionTypeUnderTest.getJavaType());
    }

    @Test
    public void testIsComparable() throws Exception
    {
        assertTrue(functionTypeUnderTest.isComparable());
    }

    @Test
    public void testIsOrderable() throws Exception
    {
        assertTrue(functionTypeUnderTest.isOrderable());
    }

    @Test
    public void testHash() throws Exception
    {
        assertEquals(0L, functionTypeUnderTest.hash(null, 0));
    }

    @Test
    public void testEqualTo() throws Exception
    {
        assertTrue(functionTypeUnderTest.equalTo(null, 0, null, 0));
    }

    @Test
    public void testCompareTo() throws Exception
    {
        assertEquals(0, functionTypeUnderTest.compareTo(null, 0, null, 0));
    }

    @Test
    public void testGetBoolean() throws Exception
    {
        assertTrue(functionTypeUnderTest.getBoolean(null, 0));
    }

    @Test
    public void testWriteBoolean() throws Exception
    {
        // Setup
        // Run the test
        functionTypeUnderTest.writeBoolean(null, false);

        // Verify the results
    }

    @Test
    public void testGetLong() throws Exception
    {
        assertEquals(0L, functionTypeUnderTest.getLong(null, 0));
    }

    @Test
    public void testWriteLong() throws Exception
    {
        // Setup
        // Run the test
        functionTypeUnderTest.writeLong(null, 0L);

        // Verify the results
    }

    @Test
    public void testGetDouble() throws Exception
    {
        assertEquals(0.0, functionTypeUnderTest.getDouble(null, 0), 0.0001);
    }

    @Test
    public void testWriteDouble() throws Exception
    {
        // Setup
        // Run the test
        functionTypeUnderTest.writeDouble(null, 0.0);

        // Verify the results
    }

    @Test
    public void testGetSlice() throws Exception
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = functionTypeUnderTest.getSlice(null, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testWriteSlice1()
    {
        // Setup
        // Run the test
        functionTypeUnderTest.writeSlice(null, null);

        // Verify the results
    }

    @Test
    public void testWriteSlice2() throws Exception
    {
        // Setup
        // Run the test
        functionTypeUnderTest.writeSlice(null, null, 0, 0);

        // Verify the results
    }

    @Test
    public void testGetObject() throws Exception
    {
        assertEquals("result", functionTypeUnderTest.getObject(null, 0));
    }

    @Test
    public void testWriteObject() throws Exception
    {
        // Setup
        // Run the test
        functionTypeUnderTest.writeObject(null, "value");

        // Verify the results
    }

    @Test
    public void testGetObjectValue() throws Exception
    {
        assertEquals("result", functionTypeUnderTest.getObjectValue(null, null, 0));
    }

    @Test
    public void testAppendTo() throws Exception
    {
        // Setup
        // Run the test
        functionTypeUnderTest.appendTo(null, 0, null);

        // Verify the results
    }

    @Test
    public void testCreateBlockBuilder1() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = functionTypeUnderTest.createBlockBuilder(null, 0, 0);

        // Verify the results
    }

    @Test
    public void testCreateBlockBuilder2() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = functionTypeUnderTest.createBlockBuilder(null, 0);

        // Verify the results
    }
}
