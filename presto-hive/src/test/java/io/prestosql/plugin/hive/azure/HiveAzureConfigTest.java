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
package io.prestosql.plugin.hive.azure;

import com.google.common.net.HostAndPort;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

public class HiveAzureConfigTest
{
    private HiveAzureConfig hiveAzureConfigUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        hiveAzureConfigUnderTest = new HiveAzureConfig();
    }

    @Test
    public void testSetWasbStorageAccount()
    {
        hiveAzureConfigUnderTest.setWasbStorageAccount("wasbStorageAccount");
        hiveAzureConfigUnderTest.setWasbAccessKey("wasbStorageAccount");
        hiveAzureConfigUnderTest.setAbfsStorageAccount("wasbStorageAccount");
        hiveAzureConfigUnderTest.setAbfsAccessKey("wasbStorageAccount");
        hiveAzureConfigUnderTest.setAdlClientId("wasbStorageAccount");
        hiveAzureConfigUnderTest.setAdlCredential("wasbStorageAccount");
        hiveAzureConfigUnderTest.setAdlRefreshUrl("wasbStorageAccount");
        hiveAzureConfigUnderTest.setAbfsOAuthClientEndpoint("wasbStorageAccount");
        hiveAzureConfigUnderTest.setAbfsOAuthClientId("wasbStorageAccount");
        hiveAzureConfigUnderTest.setAbfsOAuthClientSecret("wasbStorageAccount");
        hiveAzureConfigUnderTest.setAdlProxyHost(HostAndPort.fromHost("22"));
    }

    @Test
    public void testGetWasbStorageAccount()
    {
        Optional<String> wasbStorageAccount = hiveAzureConfigUnderTest.getWasbStorageAccount();
    }

    @Test
    public void testGetWasbAccessKey()
    {
        Optional<String> wasbAccessKey = hiveAzureConfigUnderTest.getWasbAccessKey();
    }

    @Test
    public void testGetAbfsStorageAccount()
    {
        hiveAzureConfigUnderTest.getAbfsStorageAccount();
    }

    @Test
    public void testGetAbfsAccessKey()
    {
        hiveAzureConfigUnderTest.getAbfsAccessKey();
    }

    @Test
    public void testGetAdlClientId()
    {
        hiveAzureConfigUnderTest.getAdlClientId();
    }

    @Test
    public void testGetAdlCredential()
    {
        hiveAzureConfigUnderTest.getAdlCredential();
    }

    @Test
    public void testGetAdlRefreshUrl()
    {
        hiveAzureConfigUnderTest.getAdlRefreshUrl();
    }

    @Test
    public void testGetAdlProxyHost()
    {
        hiveAzureConfigUnderTest.getAdlProxyHost();
    }

    @Test
    public void testGetAbfsOAuthClientEndpoint()
    {
        hiveAzureConfigUnderTest.getAbfsOAuthClientEndpoint();
    }

    @Test
    public void testGetAbfsOAuthClientId()
    {
        hiveAzureConfigUnderTest.getAbfsOAuthClientId();
    }

    @Test
    public void testGetAbfsOAuthClientSecret()
    {
        hiveAzureConfigUnderTest.getAbfsOAuthClientSecret();
    }
}
