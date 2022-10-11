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
package io.prestosql.plugin.hive;

import io.prestosql.spi.type.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.mockito.MockitoAnnotations.initMocks;

public class HiveColumnProjectionInfoTest
{
    @Mock
    private Type mockType;

    private HiveColumnProjectionInfo hiveColumnProjectionInfoUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hiveColumnProjectionInfoUnderTest = new HiveColumnProjectionInfo(Arrays.asList(0),
                Arrays.asList("value"), HiveType.HIVE_BOOLEAN, mockType);
    }

    @Test
    public void testHashCode()
    {
        hiveColumnProjectionInfoUnderTest.hashCode();
    }

    @Test
    public void testEquals() throws Exception
    {
        hiveColumnProjectionInfoUnderTest.equals("obj");
    }

    @Test
    public void testGetRetainedSizeInBytes()
    {
        // Setup
        // Run the test
        final long result = hiveColumnProjectionInfoUnderTest.getRetainedSizeInBytes();
    }

    @Test
    public void testGeneratePartialName()
    {
        HiveColumnProjectionInfo.generatePartialName(Arrays.asList("value"));
    }

    @Test
    public void test()
    {
        hiveColumnProjectionInfoUnderTest.getPartialName();
        hiveColumnProjectionInfoUnderTest.getDereferenceIndices();
        hiveColumnProjectionInfoUnderTest.getDereferenceNames();
        hiveColumnProjectionInfoUnderTest.getHiveType();
        hiveColumnProjectionInfoUnderTest.getType();
    }
}
