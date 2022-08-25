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
package io.prestosql.plugin.hive;

import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.type.TimeZoneKey;
import org.apache.hadoop.fs.Path;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class VacuumCleanerTest
{
    @Mock
    private VacuumTableInfoForCleaner mockVacuumTableInfo;
    @Mock
    private SemiTransactionalHiveMetastore mockMetastore;
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;

    private VacuumCleaner vacuumCleanerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        VacuumTableInfoForCleaner vacuumTableInfoForCleaner = new VacuumTableInfoForCleaner("dbname", "tablename", "partitionname", 1, new Path("name", "name"));
        vacuumCleanerUnderTest = new VacuumCleaner(vacuumTableInfoForCleaner, mockMetastore, mockHdfsEnvironment,
                new HdfsEnvironment.HdfsContext(new ConnectorSession(), "schemaName", "tableName"));
    }

    public static class ConnectorSession
            implements io.prestosql.spi.connector.ConnectorSession
    {
        @Override
        public String getQueryId()
        {
            return "queryId";
        }

        @Override
        public Optional<String> getSource()
        {
            return Optional.empty();
        }

        @Override
        public ConnectorIdentity getIdentity()
        {
            return new ConnectorIdentity("value", Optional.empty(), Optional.empty());
        }

        @Override
        public TimeZoneKey getTimeZoneKey()
        {
            return null;
        }

        @Override
        public Locale getLocale()
        {
            return null;
        }

        @Override
        public Optional<String> getTraceToken()
        {
            return Optional.empty();
        }

        @Override
        public long getStartTime()
        {
            return 0;
        }

        @Override
        public <T> T getProperty(String name, Class<T> type)
        {
            Boolean b = false;
            return (T) b;
        }
    }

    @Test
    public void testSubmitVacuumCleanupTask()
    {
        // Setup
        vacuumCleanerUnderTest.submitVacuumCleanupTask();

        // Verify the results
    }

    @Test
    public void testStopScheduledCleanupTask()
    {
        // Run the test
        vacuumCleanerUnderTest.stopScheduledCleanupTask();

        // Verify the results
    }
}
