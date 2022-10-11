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
package io.prestosql.spi.service;

import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PropertyServiceTest
{
    @Test
    public void testGetStringProperty() throws Exception
    {
        assertEquals("result", PropertyService.getStringProperty("key"));
    }

    @Test
    public void testGetBooleanProperty() throws Exception
    {
        assertTrue(PropertyService.getBooleanProperty("key"));
    }

    @Test
    public void testGetDoubleProperty() throws Exception
    {
        assertEquals(0.0, PropertyService.getDoubleProperty("key"), 0.0001);
    }

    @Test
    public void testGetList() throws Exception
    {
        assertEquals(Arrays.asList("value"), PropertyService.getList("key", 'a'));
        assertEquals(Collections.emptyList(), PropertyService.getList("key", 'a'));
    }

    @Test
    public void testGetCommaSeparatedList() throws Exception
    {
        assertEquals(Arrays.asList("value"), PropertyService.getCommaSeparatedList("key"));
        assertEquals(Collections.emptyList(), PropertyService.getCommaSeparatedList("key"));
    }

    @Test
    public void testGetDurationProperty() throws Exception
    {
        // Setup
        final Duration expectedResult = new Duration(0.0, TimeUnit.MILLISECONDS);

        // Run the test
        final Duration result = PropertyService.getDurationProperty("key");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testSetProperty() throws Exception
    {
        // Setup
        // Run the test
        PropertyService.setProperty("key", "property");

        // Verify the results
    }

    @Test
    public void testContainsProperty() throws Exception
    {
        assertTrue(PropertyService.containsProperty("key"));
    }
}
