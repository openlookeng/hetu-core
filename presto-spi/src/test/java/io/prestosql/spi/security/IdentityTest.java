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
package io.prestosql.spi.security;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class IdentityTest
{
    private Identity identityUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        identityUnderTest = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
    }

    @Test
    public void testToConnectorIdentity1() throws Exception
    {
        // Setup
        final ConnectorIdentity expectedResult = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))),
                new HashMap<>());

        // Run the test
        final ConnectorIdentity result = identityUnderTest.toConnectorIdentity();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testToConnectorIdentity2() throws Exception
    {
        // Setup
        final ConnectorIdentity expectedResult = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))),
                new HashMap<>());

        // Run the test
        final ConnectorIdentity result = identityUnderTest.toConnectorIdentity("catalog");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(identityUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, identityUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = identityUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testOfUser()
    {
        // Run the test
        final Identity result = Identity.ofUser("user");
        assertEquals("user", result.getUser());
        assertEquals(new HashSet<>(Arrays.asList("value")), result.getGroups());
        assertEquals(Optional.empty(), result.getPrincipal());
        assertEquals(new HashMap<>(), result.getRoles());
        assertEquals(new HashMap<>(), result.getExtraCredentials());
        assertEquals(new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                        Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>()),
                result.toConnectorIdentity());
        assertEquals(new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                        Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>()),
                result.toConnectorIdentity("catalog"));
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
    }

    @Test
    public void testForUser() throws Exception
    {
        // Setup
        // Run the test
        final Identity.Builder result = Identity.forUser("user");

        // Verify the results
    }

    @Test
    public void testFrom() throws Exception
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());

        // Run the test
        final Identity.Builder result = Identity.from(identity);

        // Verify the results
    }
}
