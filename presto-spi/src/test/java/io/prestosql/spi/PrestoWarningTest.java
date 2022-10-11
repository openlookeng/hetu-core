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
package io.prestosql.spi;

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PrestoWarningTest
{
    @Mock
    private WarningCode mockWarningCode;

    private PrestoWarning prestoWarningUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        prestoWarningUnderTest = new PrestoWarning(mockWarningCode, "message");
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(prestoWarningUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, prestoWarningUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", prestoWarningUnderTest.toString());
    }
}
