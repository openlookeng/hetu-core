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

import org.mockito.Mock;

import java.lang.invoke.MethodHandle;

public class MapTypeTest
{
    @Mock
    private MethodHandle mockKeyBlockNativeEquals;
    @Mock
    private MethodHandle mockKeyBlockEquals;
    @Mock
    private MethodHandle mockKeyNativeHashCode;
    @Mock
    private MethodHandle mockKeyBlockHashCode;

//    private MapType<K, V> mapTypeUnderTest;
//
//    @BeforeMethod
//    public void setUp() throws Exception
//    {
//        initMocks(this);
//        mapTypeUnderTest = new MapType<>(null, null, mockKeyBlockNativeEquals, mockKeyBlockEquals,
//                mockKeyNativeHashCode, mockKeyBlockHashCode);
//    }

//    @Test
//    public void testCreateBlockBuilder1() throws Exception
//    {
//        // Setup
//        final BlockBuilderStatus blockBuilderStatus = null;
//
//        // Run the test
//        final BlockBuilder result = mapTypeUnderTest.createBlockBuilder(blockBuilderStatus, 0, 0);
//
//        // Verify the results
//    }
//
//    @Test
//    public void testCreateBlockBuilder2() throws Exception
//    {
//        // Setup
//        final BlockBuilderStatus blockBuilderStatus = null;
//
//        // Run the test
//        final BlockBuilder result = mapTypeUnderTest.createBlockBuilder(blockBuilderStatus, 0);
//
//        // Verify the results
//    }
//
//    @Test
//    public void testIsComparable() throws Exception
//    {
//        // Setup
//        // Run the test
//        final boolean result = mapTypeUnderTest.isComparable();
//
//        // Verify the results
//        assertTrue(result);
//    }
//
//    @Test
//    public void testHash() throws Exception
//    {
//        // Setup
//        final Block block = null;
//
//        // Run the test
//        final long result = mapTypeUnderTest.hash(block, 0);
//
//        // Verify the results
//        assertEquals(0L, result);
//    }
//
//    @Test
//    public void testEqualTo() throws Exception
//    {
//        // Setup
//        final Block<T> leftBlock = null;
//        final Block<T> rightBlock = null;
//
//        // Run the test
//        final boolean result = mapTypeUnderTest.equalTo(leftBlock, 0, rightBlock, 0);
//
//        // Verify the results
//        assertTrue(result);
//    }
//
//    @Test
//    public void testCompareTo() throws Exception
//    {
//        assertEquals(0, mapTypeUnderTest.compareTo(null, 0, null, 0));
//    }
//
//    @Test
//    public void testGetObjectValue() throws Exception
//    {
//        // Setup
//        final ConnectorSession session = null;
//        final Block<T> block = null;
//
//        // Run the test
//        final Object result = mapTypeUnderTest.getObjectValue(session, block, 0);
//
//        // Verify the results
//    }
//
//    @Test
//    public void testAppendTo() throws Exception
//    {
//        // Setup
//        final Block block = null;
//        final BlockBuilder blockBuilder = null;
//
//        // Run the test
//        mapTypeUnderTest.appendTo(block, 0, blockBuilder);
//
//        // Verify the results
//    }
//
//    @Test
//    public void testGetObject() throws Exception
//    {
//        // Setup
//        final Block<T> block = null;
//
//        // Run the test
//        final Block result = mapTypeUnderTest.getObject(block, 0);
//
//        // Verify the results
//    }
//
//    @Test
//    public void testWriteObject() throws Exception
//    {
//        // Setup
//        final BlockBuilder blockBuilder = null;
//
//        // Run the test
//        mapTypeUnderTest.writeObject(blockBuilder, "value");
//
//        // Verify the results
//    }
//
//    @Test
//    public void testGetTypeParameters() throws Exception
//    {
//        // Setup
//        // Run the test
//        final List<Type> result = mapTypeUnderTest.getTypeParameters();
//
//        // Verify the results
//    }
//
//    @Test
//    public void testGetDisplayName() throws Exception
//    {
//        // Setup
//        // Run the test
//        final String result = mapTypeUnderTest.getDisplayName();
//
//        // Verify the results
//        assertEquals("result", result);
//    }
//
//    @Test
//    public void testCreateBlockFromKeyValue() throws Exception
//    {
//        // Setup
//        final Block keyBlock = null;
//        final Block valueBlock = null;
//
//        // Run the test
//        final Block result = mapTypeUnderTest.createBlockFromKeyValue(Optional.of(new boolean[]{false}), new int[]{0},
//                keyBlock, valueBlock);
//
//        // Verify the results
//    }
}
