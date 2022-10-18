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

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GcsAccessTokenProviderTest
{
    private GcsAccessTokenProvider gcsAccessTokenProviderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        gcsAccessTokenProviderUnderTest = new GcsAccessTokenProvider();
    }

    @Test
    public void testGetAccessToken()
    {
        // Setup
        // Run the test
        final Configuration configuration = new Configuration(false);

        // Run the test
        gcsAccessTokenProviderUnderTest.setConf(configuration);
        final AccessTokenProvider.AccessToken result = gcsAccessTokenProviderUnderTest.getAccessToken();

        // Verify the results
    }

    @Test
    public void testRefresh()
    {
        // Setup
        // Run the test
        gcsAccessTokenProviderUnderTest.refresh();

        // Verify the results
    }

    @Test
    public void testRefresh_ThrowsIOException()
    {
        // Setup
        // Run the test
        gcsAccessTokenProviderUnderTest.refresh();
    }

    @Test
    public void testGetConf()
    {
        // Setup
        // Run the test
        final Configuration result = gcsAccessTokenProviderUnderTest.getConf();

        // Verify the results
    }
}
