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
package io.prestosql.plugin.hive.authentication;

import com.google.inject.Module;
import org.testng.annotations.Test;

public class AuthenticationModulesTest
{
    @Test
    public void testNoHiveMetastoreAuthenticationModule()
    {
        // Setup
        // Run the test
        final Module result = AuthenticationModules.noHiveMetastoreAuthenticationModule();
    }

    @Test
    public void testKerberosHiveMetastoreAuthenticationModule()
    {
        // Setup
        // Run the test
        final Module result = AuthenticationModules.kerberosHiveMetastoreAuthenticationModule();
    }

    @Test
    public void testNoHdfsAuthenticationModule()
    {
        // Setup
        // Run the test
        final Module result = AuthenticationModules.noHdfsAuthenticationModule();
    }

    @Test
    public void testSimpleImpersonatingHdfsAuthenticationModule()
    {
        // Setup
        // Run the test
        final Module result = AuthenticationModules.simpleImpersonatingHdfsAuthenticationModule();
    }

    @Test
    public void testKerberosHdfsAuthenticationModule()
    {
        // Setup
        // Run the test
        final Module result = AuthenticationModules.kerberosHdfsAuthenticationModule();
    }

    @Test
    public void testKerberosImpersonatingHdfsAuthenticationModule()
    {
        // Setup
        // Run the test
        final Module result = AuthenticationModules.kerberosImpersonatingHdfsAuthenticationModule();
    }
}
