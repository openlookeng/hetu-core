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
package io.prestosql.plugin.hive.gcs;

import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.VacuumCleanerTest;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;

public class GcsConfigurationProviderTest
{
    private GcsConfigurationProvider gcsConfigurationProviderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        gcsConfigurationProviderUnderTest = new GcsConfigurationProvider();
    }

    @Test
    public void testUpdateConfiguration() throws Exception
    {
        // Setup
        final Configuration configuration = new Configuration(false);
        final HdfsEnvironment.HdfsContext context = new HdfsEnvironment.HdfsContext(new VacuumCleanerTest.ConnectorSession(), "schemaName", "tableName");

        // Run the test
        gcsConfigurationProviderUnderTest.updateConfiguration(configuration, context, new URI("https://example.com/"));
        gcsConfigurationProviderUnderTest.updateConfiguration(configuration, context, new URI("gs://example.com/"));

        // Verify the results
    }
}
