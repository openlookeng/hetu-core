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

import io.prestosql.spi.connector.ConnectorFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IcebergPluginTest
{
    private IcebergPlugin icebergPluginUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        icebergPluginUnderTest = new IcebergPlugin();
    }

    @Test
    public void testGetConnectorFactories()
    {
        // Setup
        // Run the test
        final Iterable<ConnectorFactory> result = icebergPluginUnderTest.getConnectorFactories();

        // Verify the results
    }
}
