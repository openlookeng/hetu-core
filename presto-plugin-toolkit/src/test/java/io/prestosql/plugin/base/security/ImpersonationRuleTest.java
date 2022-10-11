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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;

public class ImpersonationRuleTest
{
    private ImpersonationRule impersonationRuleUnderTest;

    private static final Pattern REGEX = Pattern.compile("regex");

    @BeforeMethod
    public void setUp() throws Exception
    {
        impersonationRuleUnderTest = new ImpersonationRule(REGEX, REGEX, false);
    }

    @Test
    public void testMatch() throws Exception
    {
        // Setup
        // Run the test
        final Optional<Boolean> result = impersonationRuleUnderTest.match("originalUser", "newUser");

        // Verify the results
        assertEquals(Optional.of(false), result);
    }
}
