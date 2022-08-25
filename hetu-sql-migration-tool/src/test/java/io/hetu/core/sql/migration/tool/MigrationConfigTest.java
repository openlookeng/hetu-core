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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

public class MigrationConfigTest
{
    private MigrationConfig migrationConfigUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        Console consoleUnderTest = new Console();
        consoleUnderTest.cliOptions = mock(CliOptions.class);
        migrationConfigUnderTest = new MigrationConfig(consoleUnderTest.cliOptions.configFile);
    }

    @Test
    public void testIsConvertDecimalAsDouble()
    {
        // Setup
        // Run the test
        final boolean result = migrationConfigUnderTest.isConvertDecimalAsDouble();
    }
}
