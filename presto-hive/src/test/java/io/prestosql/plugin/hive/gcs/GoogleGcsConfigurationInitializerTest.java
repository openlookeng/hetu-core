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

import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.MockitoAnnotations.initMocks;

public class GoogleGcsConfigurationInitializerTest
{
    @Mock
    private HiveGcsConfig mockConfig;

    private GoogleGcsConfigurationInitializer googleGcsConfigurationInitializerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        HiveGcsConfig hiveGcsConfig = new HiveGcsConfig().setUseGcsAccessToken(true);
        HiveGcsConfig path = hiveGcsConfig.setJsonKeyFilePath("path");
        googleGcsConfigurationInitializerUnderTest = new GoogleGcsConfigurationInitializer(path);
    }

    @Test
    public void testInitializeConfiguration()
    {
        // Setup
        final Configuration config = new Configuration(true);

        // Run the test
        googleGcsConfigurationInitializerUnderTest.initializeConfiguration(config);

        HiveGcsConfig path = new HiveGcsConfig().setJsonKeyFilePath("path");
        GoogleGcsConfigurationInitializer googleGcsConfigurationInitializer = new GoogleGcsConfigurationInitializer(path);
        googleGcsConfigurationInitializer.initializeConfiguration(config);

        // Verify the results
    }
}
