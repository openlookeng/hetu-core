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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class MapParametricTypeTest
{
    private MapParametricType mapParametricTypeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        mapParametricTypeUnderTest = new MapParametricType();
    }

    @Test
    public void testGetName() throws Exception
    {
        assertEquals("map", mapParametricTypeUnderTest.getName());
    }

    @Test
    public void testCreateType() throws Exception
    {
        // Setup
        final TypeManager typeManager = null;
        final List<TypeParameter> parameters = Arrays.asList(TypeParameter.of(0L));

        // Run the test
        final Type result = mapParametricTypeUnderTest.createType(typeManager, parameters);

        // Verify the results
    }
}
