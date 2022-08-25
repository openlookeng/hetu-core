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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SingleRowBlockTest
{
    private SingleRowBlock<Object> singleRowBlockUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        singleRowBlockUnderTest = new SingleRowBlock<>(0, new Block[]{});
    }

    @Test
    public void testGetNumFields() throws Exception
    {
        assertEquals(0, singleRowBlockUnderTest.getNumFields());
    }

    @Test
    public void testGetRawFieldBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = singleRowBlockUnderTest.getRawFieldBlock(0);

        // Verify the results
    }

    @Test
    public void testGetPositionCount() throws Exception
    {
        assertEquals(0, singleRowBlockUnderTest.getPositionCount());
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        assertEquals(0L, singleRowBlockUnderTest.getSizeInBytes());
    }

    @Test
    public void testGetRetainedSizeInBytes() throws Exception
    {
        assertEquals(0L, singleRowBlockUnderTest.getRetainedSizeInBytes());
    }

    @Test
    public void testRetainedBytesForEachPart() throws Exception
    {
        // Setup
        final BiConsumer<Object, Long> mockConsumer = mock(BiConsumer.class);

        // Run the test
        singleRowBlockUnderTest.retainedBytesForEachPart(mockConsumer);

        // Verify the results
    }

    @Test
    public void testGetEncodingName() throws Exception
    {
        assertEquals("ROW_ELEMENT", singleRowBlockUnderTest.getEncodingName());
    }

    @Test
    public void testGetRowIndex() throws Exception
    {
        assertEquals(0, singleRowBlockUnderTest.getRowIndex());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", singleRowBlockUnderTest.toString());
    }

    @Test
    public void testGetLoadedBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = singleRowBlockUnderTest.getLoadedBlock();

        // Verify the results
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(singleRowBlockUnderTest.equals("obj"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, singleRowBlockUnderTest.hashCode());
    }
}
