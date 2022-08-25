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
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.connector.ConnectorSession;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AbstractTypeTest
{
    @Mock
    private TypeSignature mockSignature;

    private AbstractType abstractTypeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        abstractTypeUnderTest = new AbstractType(mockSignature, Object.class)
        {
            @Override
            public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
            {
                return null;
            }

            @Override
            public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
            {
                return null;
            }

            @Override
            public <T> T get(Block<T> block, int position)
            {
                return null;
            }

            @Override
            public <T> Object getObjectValue(ConnectorSession session, Block<T> block, int position)
            {
                return null;
            }

            @Override
            public void appendTo(Block block, int position, BlockBuilder blockBuilder)
            {
            }

            @Override
            public Optional<Range> getRange()
            {
                return null;
            }

            @Override
            public <T> T read(RawInput<T> input)
            {
                return null;
            }

            @Override
            public <T> void write(BlockBuilder<T> blockBuilder, T value)
            {
            }

            @Override
            public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
            {
                return null;
            }
        };
    }

    @Test
    public void testGetTypeSignature() throws Exception
    {
        // Setup
        final TypeSignature expectedResult = new TypeSignature("base", TypeSignatureParameter.of(0L));

        // Run the test
        final TypeSignature result = abstractTypeUnderTest.getTypeSignature();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetDisplayName() throws Exception
    {
        // Setup
        when(mockSignature.toString()).thenReturn("result");

        // Run the test
        final String result = abstractTypeUnderTest.getDisplayName();

        // Verify the results
        assertEquals("signature", result);
    }

    @Test
    public void testGetTypeParameters() throws Exception
    {
        // Setup
        // Run the test
        final List<Type> result = abstractTypeUnderTest.getTypeParameters();

        // Verify the results
    }

    @Test
    public void testIsComparable() throws Exception
    {
        assertTrue(abstractTypeUnderTest.isComparable());
    }

    @Test
    public void testIsOrderable() throws Exception
    {
        assertTrue(abstractTypeUnderTest.isOrderable());
    }

    @Test
    public void testGetObject() throws Exception
    {
        assertEquals("result", abstractTypeUnderTest.getObject(null, 0));
    }

    @Test
    public void testGetBoolean() throws Exception
    {
        assertTrue(abstractTypeUnderTest.getBoolean(null, 0));
    }

    @Test
    public void testWriteBoolean() throws Exception
    {
        // Setup
        // Run the test
        abstractTypeUnderTest.writeBoolean(null, false);

        // Verify the results
    }

    @Test
    public void testGetLong() throws Exception
    {
        assertEquals(0L, abstractTypeUnderTest.getLong(null, 0));
    }

    @Test
    public void testWriteLong() throws Exception
    {
        // Setup
        // Run the test
        abstractTypeUnderTest.writeLong(null, 0L);

        // Verify the results
    }

    @Test
    public void testGetDouble() throws Exception
    {
        assertEquals(0.0, abstractTypeUnderTest.getDouble(null, 0), 0.0001);
    }

    @Test
    public void testWriteDouble() throws Exception
    {
        // Setup
        // Run the test
        abstractTypeUnderTest.writeDouble(null, 0.0);

        // Verify the results
    }

    @Test
    public void testGetSlice() throws Exception
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = abstractTypeUnderTest.getSlice(null, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testWriteSlice1()
    {
        // Setup
        // Run the test
        abstractTypeUnderTest.writeSlice(null, null);

        // Verify the results
    }

    @Test
    public void testWriteSlice2() throws Exception
    {
        // Setup
        // Run the test
        abstractTypeUnderTest.writeSlice(null, null, 0, 0);

        // Verify the results
    }

    @Test
    public void testWriteObject() throws Exception
    {
        // Setup
        // Run the test
        abstractTypeUnderTest.writeObject(null, "value");

        // Verify the results
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        when(mockSignature.toString()).thenReturn("result");

        // Run the test
        final String result = abstractTypeUnderTest.toString();

        // Verify the results
        assertEquals("signature", result);
    }

    @Test
    public void testEquals() throws Exception
    {
        // Setup
        when(mockSignature.equals(new TypeSignature("base", TypeSignatureParameter.of(0L)))).thenReturn(false);

        // Run the test
        final boolean result = abstractTypeUnderTest.equals("o");

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testHashCode() throws Exception
    {
        // Setup
        when(mockSignature.hashCode()).thenReturn(0);

        // Run the test
        final int result = abstractTypeUnderTest.hashCode();

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testEqualTo() throws Exception
    {
        assertTrue(abstractTypeUnderTest.equalTo(null, 0, null, 0));
    }

    @Test
    public void testHash() throws Exception
    {
        assertEquals(0L, abstractTypeUnderTest.hash(null, 0));
    }

    @Test
    public void testCompareTo() throws Exception
    {
        assertEquals(0, abstractTypeUnderTest.compareTo(null, 0, null, 0));
    }
}
