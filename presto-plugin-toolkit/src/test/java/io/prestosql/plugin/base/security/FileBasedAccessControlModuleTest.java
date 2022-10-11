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
package io.prestosql.plugin.base.security;

import com.google.inject.Binder;
import io.airlift.units.Duration;
import io.prestosql.spi.connector.ConnectorAccessControl;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class FileBasedAccessControlModuleTest
{
    private FileBasedAccessControlModule fileBasedAccessControlModuleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        fileBasedAccessControlModuleUnderTest = new FileBasedAccessControlModule();
    }

    @Test
    public void testConfigure() throws Exception
    {
        // Setup
        final Binder binder = null;

        // Run the test
        fileBasedAccessControlModuleUnderTest.configure(binder);

        // Verify the results
    }

    @Test
    public void testGetConnectorAccessControl()
    {
        // Setup
        final FileBasedAccessControlConfig config = new FileBasedAccessControlConfig();
        config.setConfigFile("configFile");
        config.setRefreshPeriod(new Duration(0.0, TimeUnit.MILLISECONDS));

        // Run the test
        final ConnectorAccessControl result = fileBasedAccessControlModuleUnderTest.getConnectorAccessControl(config);

        // Verify the results
    }
}
