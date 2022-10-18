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
package io.prestosql.spi.block;

import io.airlift.slice.Slice;
import io.prestosql.spi.type.MapType;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SingleMapBlockTest
{
    @Mock
    private MapType mockMapType;
    @Mock
    private Block mockKeyBlock;
    @Mock
    private Block mockValueBlock;

    private SingleMapBlock<Object> singleMapBlockUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        singleMapBlockUnderTest = new SingleMapBlock<>(mockMapType, 0, 0, mockKeyBlock, mockValueBlock, new int[]{0});
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        // Setup
        when(mockKeyBlock.getRegionSizeInBytes(0, 0)).thenReturn(0L);
        when(mockValueBlock.getRegionSizeInBytes(0, 0)).thenReturn(0L);

        // Run the test
        final long result = singleMapBlockUnderTest.getSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetRetainedSizeInBytes() throws Exception
    {
        // Setup
        when(mockKeyBlock.getRetainedSizeInBytes()).thenReturn(0L);
        when(mockValueBlock.getRetainedSizeInBytes()).thenReturn(0L);

        // Run the test
        final long result = singleMapBlockUnderTest.getRetainedSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testRetainedBytesForEachPart() throws Exception
    {
        // Setup
        final BiConsumer<Object, Long> mockConsumer = mock(BiConsumer.class);
        when(mockKeyBlock.getRetainedSizeInBytes()).thenReturn(0L);
        when(mockValueBlock.getRetainedSizeInBytes()).thenReturn(0L);

        // Run the test
        singleMapBlockUnderTest.retainedBytesForEachPart(mockConsumer);

        // Verify the results
    }

    @Test
    public void testGetEncodingName() throws Exception
    {
        assertEquals("MAP_ELEMENT", singleMapBlockUnderTest.getEncodingName());
    }

    @Test
    public void testGetRawKeyBlock()
    {
        // Setup
        // Run the test
        final Block result = singleMapBlockUnderTest.getRawKeyBlock();

        // Verify the results
    }

    @Test
    public void testGetRawValueBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = singleMapBlockUnderTest.getRawValueBlock();

        // Verify the results
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", singleMapBlockUnderTest.toString());
    }

    @Test
    public void testGetLoadedBlock() throws Exception
    {
        // Setup
        when(mockKeyBlock.getLoadedBlock()).thenReturn(null);
        when(mockValueBlock.getLoadedBlock()).thenReturn(null);

        // Run the test
        final Block result = singleMapBlockUnderTest.getLoadedBlock();

        // Verify the results
    }

    @Test
    public void testGetHashTable() throws Exception
    {
        assertEquals(new int[]{0}, singleMapBlockUnderTest.getHashTable());
    }

    @Test
    public void testSeekKey()
    {
        // Setup
        when(mockMapType.getKeyNativeHashCode()).thenReturn(null);
        when(mockMapType.getKeyBlockNativeEquals()).thenReturn(null);

        // Run the test
        final int result = singleMapBlockUnderTest.seekKey("nativeValue");

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testSeekKeyExact1()
    {
        // Setup
        when(mockMapType.getKeyNativeHashCode()).thenReturn(null);
        when(mockMapType.getKeyBlockNativeEquals()).thenReturn(null);

        // Run the test
        final int result = singleMapBlockUnderTest.seekKeyExact(0L);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testSeekKeyExact2()
    {
        // Setup
        when(mockMapType.getKeyNativeHashCode()).thenReturn(null);
        when(mockMapType.getKeyBlockNativeEquals()).thenReturn(null);

        // Run the test
        final int result = singleMapBlockUnderTest.seekKeyExact(false);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testSeekKeyExact3()
    {
        // Setup
        when(mockMapType.getKeyNativeHashCode()).thenReturn(null);
        when(mockMapType.getKeyBlockNativeEquals()).thenReturn(null);

        // Run the test
        final int result = singleMapBlockUnderTest.seekKeyExact(0.0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testSeekKeyExact4()
    {
        // Setup
        final Slice nativeValue = null;
        when(mockMapType.getKeyNativeHashCode()).thenReturn(null);
        when(mockMapType.getKeyBlockNativeEquals()).thenReturn(null);

        // Run the test
        final int result = singleMapBlockUnderTest.seekKeyExact(nativeValue);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testSeekKeyExact5()
    {
        // Setup
        final Block nativeValue = null;
        when(mockMapType.getKeyNativeHashCode()).thenReturn(null);
        when(mockMapType.getKeyBlockNativeEquals()).thenReturn(null);

        // Run the test
        final int result = singleMapBlockUnderTest.seekKeyExact(nativeValue);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(singleMapBlockUnderTest.equals("obj"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, singleMapBlockUnderTest.hashCode());
    }
}
