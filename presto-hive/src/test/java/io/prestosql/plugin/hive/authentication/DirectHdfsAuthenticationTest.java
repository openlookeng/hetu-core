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
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DirectHdfsAuthenticationTest<R, E extends Exception>
{
    @Mock
    private HadoopAuthentication mockHadoopAuthentication;

    private DirectHdfsAuthentication directHdfsAuthenticationUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        directHdfsAuthenticationUnderTest = new DirectHdfsAuthentication(mockHadoopAuthentication);
    }

    @Test
    public void testDoAs() throws Exception
    {
        // Setup
        final GenericExceptionAction<R, E> action = null;

        // Run the test
        final R result = directHdfsAuthenticationUnderTest.doAs("user", action);

        // Verify the results
    }

    @Test
    public void testDoAs_ThrowsE() throws Exception
    {
        // Setup
        final GenericExceptionAction<R, E> action = null;

        // Configure HadoopAuthentication.getUserGroupInformation(...).
        final UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("user");
        when(mockHadoopAuthentication.getUserGroupInformation()).thenReturn(userGroupInformation);

        // Run the test
        directHdfsAuthenticationUnderTest.doAs("user", action);
    }
}
