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
package io.prestosql.plugin.hive.metastore.procedure;

import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.TestingMetastoreLocator;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastoreConfig;
import io.prestosql.spi.procedure.Procedure;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class FlushHiveMetastoreCacheProcedureTest
{
    private FlushHiveMetastoreCacheProcedure flushHiveMetastoreCacheProcedureUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        BridgingHiveMetastore host = new BridgingHiveMetastore(new ThriftHiveMetastore(new TestingMetastoreLocator(new HiveConfig(), "host", 22), new ThriftHiveMetastoreConfig()));
        flushHiveMetastoreCacheProcedureUnderTest = new FlushHiveMetastoreCacheProcedure(
                Optional.of(
                        new CachingHiveMetastore(host, MoreExecutors.directExecutor(), MoreExecutors.directExecutor(),
                                new Duration(0.0, TimeUnit.MILLISECONDS), new Duration(0.0, TimeUnit.MILLISECONDS),
                                new Duration(0.0, TimeUnit.MILLISECONDS), new Duration(0.0, TimeUnit.MILLISECONDS), 0L,
                                false)));
    }

    @Test
    public void testGet() throws Exception
    {
        // Setup
        // Run the test
        final Procedure result = flushHiveMetastoreCacheProcedureUnderTest.get();

        // Verify the results
    }

    @Test
    public void testFlushMetadataCache()
    {
        // Setup
        // Run the test
        flushHiveMetastoreCacheProcedureUnderTest.flushMetadataCache("procedure should only be invoked with named parameters", "schemaName", "tableName",
                Arrays.asList("value"),
                Arrays.asList("value"));

        // Verify the results
    }
}
