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
package io.hetu.core.sql.migration.tool;

import io.hetu.core.sql.migration.SqlMigrationException;
import io.hetu.core.sql.migration.SqlSyntaxType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;

public class SqlConverterFactoryTest
{
    @Test
    public void testGetSqlConverter() throws Exception
    {
        // Setup
        final SessionProperties session = new SessionProperties();
        session.setSourceType(SqlSyntaxType.HIVE);
        session.setParsingOptions(false);
        session.setConsolePrintEnable(false);
        session.setMigrationConfig(new MigrationConfig("configFile"));
        session.setDebugEnable(false);

        // Run the test
        final SqlSyntaxConverter result = SqlConverterFactory.getSqlConverter(session);

        // Verify the results
    }

    @Test
    public void testGetSqlConverter_ThrowsSqlMigrationException() throws Exception
    {
        // Setup
        final SessionProperties session = new SessionProperties();
        session.setSourceType(SqlSyntaxType.HIVE);
        session.setParsingOptions(false);
        session.setConsolePrintEnable(false);
        session.setMigrationConfig(new MigrationConfig("configFile"));
        session.setDebugEnable(false);

        // Run the test
        assertThrows(SqlMigrationException.class, () -> SqlConverterFactory.getSqlConverter(session));
    }
}
