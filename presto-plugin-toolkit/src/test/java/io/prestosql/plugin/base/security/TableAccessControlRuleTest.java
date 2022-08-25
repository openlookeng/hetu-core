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

import io.prestosql.spi.connector.SchemaTableName;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;

public class TableAccessControlRuleTest
{
    private TableAccessControlRule tableAccessControlRuleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        tableAccessControlRuleUnderTest = new TableAccessControlRule(
                new HashSet<>(Arrays.asList(TableAccessControlRule.TablePrivilege.SELECT)),
                Optional.of(Pattern.compile("regex")), Optional.of(Pattern.compile("regex")),
                Optional.of(Pattern.compile("regex")), Optional.of(Pattern.compile("regex")));
    }

    @Test
    public void testMatch() throws Exception
    {
        // Setup
        final SchemaTableName table = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final Optional<Set<TableAccessControlRule.TablePrivilege>> result = tableAccessControlRuleUnderTest.match(
                "user", new HashSet<>(
                        Arrays.asList("value")), table);

        // Verify the results
        assertEquals(Optional.of(new HashSet<>(Arrays.asList(TableAccessControlRule.TablePrivilege.SELECT))), result);
    }
}
