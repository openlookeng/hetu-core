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
package io.hetu.core.plugin.iceberg;

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class IcebergTablePropertiesTest
{
    @Mock
    private IcebergConfig mockIcebergConfig;

    private IcebergTableProperties icebergTablePropertiesUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        icebergTablePropertiesUnderTest = new IcebergTableProperties(mockIcebergConfig);
    }

    @Test
    public void testGetFileFormat()
    {
        // Setup
        final Map<String, Object> tableProperties = new HashMap<>();

        // Run the test
        final IcebergFileFormat result = IcebergTableProperties.getFileFormat(tableProperties);

        // Verify the results
        assertEquals(IcebergFileFormat.ORC, result);
    }

    @Test
    public void testGetPartitioning()
    {
        // Setup
        final Map<String, Object> tableProperties = new HashMap<>();

        // Run the test
        final List<String> result = IcebergTableProperties.getPartitioning(tableProperties);

        // Verify the results
        assertEquals(Arrays.asList("value"), result);
    }

    @Test
    public void testGetTableLocation()
    {
        // Setup
        final Map<String, Object> tableProperties = new HashMap<>();

        // Run the test
        final Optional<String> result = IcebergTableProperties.getTableLocation(tableProperties);

        // Verify the results
        assertEquals(Optional.of("value"), result);
    }
}
