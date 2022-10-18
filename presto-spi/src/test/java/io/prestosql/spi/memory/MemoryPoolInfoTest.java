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
package io.prestosql.spi.memory;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.testng.Assert.assertEquals;

public class MemoryPoolInfoTest
{
    private MemoryPoolInfo memoryPoolInfoUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        memoryPoolInfoUnderTest = new MemoryPoolInfo(0L, 0L, 0L, new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    @Test
    public void testGetFreeBytes() throws Exception
    {
        assertEquals(0L, memoryPoolInfoUnderTest.getFreeBytes());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", memoryPoolInfoUnderTest.toString());
    }
}
