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

import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class BlockBuilderStatusTest
{
    @Mock
    private PageBuilderStatus mockPageBuilderStatus;

    private BlockBuilderStatus blockBuilderStatusUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        blockBuilderStatusUnderTest = new BlockBuilderStatus(mockPageBuilderStatus);
    }

    @Test
    public void testGetMaxPageSizeInBytes() throws Exception
    {
        // Setup
        when(mockPageBuilderStatus.getMaxPageSizeInBytes()).thenReturn(0);

        // Run the test
        final int result = blockBuilderStatusUnderTest.getMaxPageSizeInBytes();

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testAddBytes() throws Exception
    {
        // Setup
        // Run the test
        blockBuilderStatusUnderTest.addBytes(0);

        // Verify the results
        verify(mockPageBuilderStatus).addBytes(0);
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", blockBuilderStatusUnderTest.toString());
    }

    @Test
    public void testCapture() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;
        when(mockPageBuilderStatus.capture(any(BlockEncodingSerdeProvider.class))).thenReturn("result");

        // Run the test
        final Object result = blockBuilderStatusUnderTest.capture(serdeProvider);

        // Verify the results
    }

    @Test
    public void testRestore() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        blockBuilderStatusUnderTest.restore("state", serdeProvider);

        // Verify the results
        verify(mockPageBuilderStatus).restore(any(Object.class), any(BlockEncodingSerdeProvider.class));
    }
}
