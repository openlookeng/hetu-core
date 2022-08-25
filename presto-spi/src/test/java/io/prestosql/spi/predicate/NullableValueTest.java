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
package io.prestosql.spi.predicate;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class NullableValueTest
{
    @Mock
    private Type mockType;

    private NullableValue nullableValueUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        nullableValueUnderTest = new NullableValue(mockType, "value");
    }

    @Test
    public void testGetSerializable() throws Exception
    {
        // Setup
        // Run the test
        final NullableValue.Serializable result = nullableValueUnderTest.getSerializable();

        // Verify the results
    }

    @Test
    public void testAsBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = nullableValueUnderTest.asBlock();

        // Verify the results
    }

    @Test
    public void testIsNull() throws Exception
    {
        assertTrue(nullableValueUnderTest.isNull());
    }

    @Test
    public void testHashCode() throws Exception
    {
        // Setup
        when(mockType.hash(any(Block.class), eq(0))).thenReturn(0L);

        // Run the test
        final int result = nullableValueUnderTest.hashCode();

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testEquals() throws Exception
    {
        // Setup
        when(mockType.equalTo(any(Block.class), eq(0), any(Block.class), eq(0))).thenReturn(false);

        // Run the test
        final boolean result = nullableValueUnderTest.equals("obj");

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", nullableValueUnderTest.toString());
    }

    @Test
    public void testOf() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final NullableValue result = NullableValue.of(type, "value");
        assertEquals(new NullableValue.Serializable(null, null), result.getSerializable());
        assertEquals(null, result.asBlock());
        assertEquals(null, result.getType());
        assertTrue(result.isNull());
        assertEquals("value", result.getValue());
        assertEquals(0, result.hashCode());
        assertTrue(result.equals("obj"));
        assertEquals("result", result.toString());
    }

    @Test
    public void testAsNull() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final NullableValue result = NullableValue.asNull(type);
        assertEquals(new NullableValue.Serializable(null, null), result.getSerializable());
        assertEquals(null, result.asBlock());
        assertEquals(null, result.getType());
        assertTrue(result.isNull());
        assertEquals("value", result.getValue());
        assertEquals(0, result.hashCode());
        assertTrue(result.equals("obj"));
        assertEquals("result", result.toString());
    }

    @Test
    public void testFromSerializable() throws Exception
    {
        // Setup
        final NullableValue.Serializable serializable = new NullableValue.Serializable(null, null);

        // Run the test
        final NullableValue result = NullableValue.fromSerializable(serializable);
        assertEquals(new NullableValue.Serializable(null, null), result.getSerializable());
        assertEquals(null, result.asBlock());
        assertEquals(null, result.getType());
        assertTrue(result.isNull());
        assertEquals("value", result.getValue());
        assertEquals(0, result.hashCode());
        assertTrue(result.equals("obj"));
        assertEquals("result", result.toString());
    }
}
