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

import io.prestosql.plugin.hive.VacuumCleanerTest;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.security.ConnectorIdentity;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.MockitoAnnotations.initMocks;

public class HiveIdentityTest
{
    @Mock
    private ConnectorSession mockSession;

    private HiveIdentity hiveIdentityUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hiveIdentityUnderTest = new HiveIdentity(new VacuumCleanerTest.ConnectorSession());
        HiveIdentity user = new HiveIdentity(ConnectorIdentity.ofUser("user"));
    }

    @Test
    public void testToString() throws Exception
    {
        hiveIdentityUnderTest.toString();
    }

    @Test
    public void testEquals() throws Exception
    {
        hiveIdentityUnderTest.equals("o");
    }

    @Test
    public void testHashCode()
    {
        hiveIdentityUnderTest.hashCode();
    }

    @Test
    public void testNone() throws Exception
    {
        // Run the test
        final HiveIdentity result = HiveIdentity.none();
        result.getUsername();
    }
}
