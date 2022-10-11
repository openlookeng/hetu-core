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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.regex.Pattern;

public class AccessControlRulesTest
{
    private AccessControlRules accessControlRulesUnderTest;

    private static final Pattern REGEX = Pattern.compile("regex");

    @BeforeMethod
    public void setUp() throws Exception
    {
        accessControlRulesUnderTest = new AccessControlRules(Optional.of(
                Arrays.asList(new SchemaAccessControlRule(false, Optional.of(REGEX), Optional.of(
                        REGEX), Optional.of(REGEX)))),
                Optional.of(Arrays.asList(new TableAccessControlRule(
                        new HashSet<>(Arrays.asList(TableAccessControlRule.TablePrivilege.SELECT)), Optional.of(
                        REGEX), Optional.of(REGEX), Optional.of(
                        REGEX), Optional.of(REGEX)))),
                Optional.of(Arrays.asList(new SessionPropertyAccessControlRule(false, Optional.of(
                        REGEX), Optional.of(REGEX), Optional.of(
                        REGEX)))));
    }
}
