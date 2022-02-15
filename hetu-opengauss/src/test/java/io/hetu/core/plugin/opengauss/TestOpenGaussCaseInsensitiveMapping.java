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

package io.hetu.core.plugin.opengauss;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.postgresql.TestPostgreSqlCaseInsensitiveMapping;
import org.testng.annotations.Test;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestOpenGaussCaseInsensitiveMapping
        extends TestPostgreSqlCaseInsensitiveMapping
{
    public TestOpenGaussCaseInsensitiveMapping()
    {
        this(new TestOpenGaussServer());
    }

    public TestOpenGaussCaseInsensitiveMapping(TestOpenGaussServer postgreSqlServer)
    {
        super(() -> OpenGaussQueryRunner.createOpenGaussQueryRunner(
                postgreSqlServer,
                ImmutableMap.of("case-insensitive-name-matching", "true"),
                ImmutableSet.of()), postgreSqlServer);
    }

    @Test
    @Override
    public void testNonLowerCaseTableName()
            throws Exception
    {
        try (AutoCloseable ignore1 = withSchema("\"SomeSchema\"");
                AutoCloseable ignore2 = withTable(
                        "\"SomeSchema\".\"NonLowerCaseTable\"", "AS SELECT * FROM (VALUES ('a', 'b', 'c')) t(lower_case_name, \"Mixed_Case_Name\", \"UPPER_CASE_NAME\")")) {
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'someschema' AND table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'nonlowercasetable'",
                    "VALUES 'lower_case_name', 'mixed_case_name', 'upper_case_name'");
            assertEquals(
                    computeActual("SHOW COLUMNS FROM someschema.nonlowercasetable").getMaterializedRows().stream()
                            .map(row -> row.getField(0))
                            .collect(toImmutableSet()),
                    ImmutableSet.of("lower_case_name", "mixed_case_name", "upper_case_name"));

            assertQuery("SELECT lower_case_name FROM someschema.nonlowercasetable", "VALUES 'a'");
            assertQuery("SELECT mixed_case_name FROM someschema.nonlowercasetable", "VALUES 'b'");
            assertQuery("SELECT upper_case_name FROM someschema.nonlowercasetable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM SomeSchema.NonLowerCaseTable", "VALUES 'c'");
            assertQuery("SELECT upper_case_name FROM \"SomeSchema\".\"NonLowerCaseTable\"", "VALUES 'c'");

            assertUpdate("INSERT INTO someschema.nonlowercasetable (lower_case_name) VALUES ('lower')", 1);
            assertUpdate("INSERT INTO someschema.nonlowercasetable (mixed_case_name) VALUES ('mixed')", 1);
            assertUpdate("INSERT INTO someschema.nonlowercasetable (upper_case_name) VALUES ('upper')", 1);
            assertQuery(
                    "SELECT * FROM someschema.nonlowercasetable",
                    "VALUES ('a', 'b', 'c')," +
                            "('lower', NULL, NULL)," +
                            "(NULL, 'mixed', NULL)," +
                            "(NULL, NULL, 'upper')");
        }
    }
}
