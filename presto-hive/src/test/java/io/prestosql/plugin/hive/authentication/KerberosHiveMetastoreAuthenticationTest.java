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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TTransport;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class KerberosHiveMetastoreAuthenticationTest
{
    @Mock
    private MetastoreKerberosConfig mockConfig;
    @Mock
    private HadoopAuthentication mockAuthentication;

    private KerberosHiveMetastoreAuthentication kerberosHiveMetastoreAuthenticationUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        kerberosHiveMetastoreAuthenticationUnderTest = new KerberosHiveMetastoreAuthentication(mockConfig,
                mockAuthentication);
    }

    @Test
    public void testAuthenticate()
    {
        // Setup
        final TTransport rawTransport = null;

        // Configure HadoopAuthentication.getUserGroupInformation(...).
        final UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("user");
        when(mockAuthentication.getUserGroupInformation()).thenReturn(userGroupInformation);

        // Run the test
        final TTransport result = kerberosHiveMetastoreAuthenticationUnderTest.authenticate(rawTransport,
                "hiveMetastoreHost");

        // Verify the results
    }
}
