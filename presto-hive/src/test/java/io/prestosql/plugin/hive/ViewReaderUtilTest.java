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

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.MetadataProvider;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;

public class ViewReaderUtilTest
{
    @Test
    public void testCreateViewReader()
    {
        // Setup
        final HiveConfig hiveConfig = new HiveConfig();
        hiveConfig.setTargetMaxFileSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxInitialSplits(0);
        hiveConfig.setMaxInitialSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setSplitLoaderConcurrency(0);
        hiveConfig.setMaxSplitsPerSecond(0);
        hiveConfig.setDomainCompactionThreshold(0);
        hiveConfig.setWriterSortBufferSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setForceLocalScheduling(false);
        hiveConfig.setMaxConcurrentFileRenames(0);
        hiveConfig.setRecursiveDirWalkerEnabled(false);
        hiveConfig.setMaxSplitSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxPartitionsPerScan(0);
        hiveConfig.setMaxOutstandingSplits(0);
        hiveConfig.setMaxOutstandingSplitsSize(new DataSize(0.0, DataSize.Unit.BYTE));
        hiveConfig.setMaxSplitIteratorThreads(0);
        HiveConfig hdfsConfig = new HiveConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        FileHiveMetastore name = FileHiveMetastore.createTestingFileHiveMetastore(new File("name"));
        final SemiTransactionalHiveMetastore metastore = new SemiTransactionalHiveMetastore(
                new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication()),
                name,
                MoreExecutors.directExecutor(),
                io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE,
                new Duration(0.0, TimeUnit.MILLISECONDS),
                false,
                false,
                Optional.of(new Duration(0.0, TimeUnit.MILLISECONDS)),
                io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE,
                io.prestosql.hadoop.$internal.io.netty.util.concurrent.GlobalEventExecutor.INSTANCE,
                0);
        final ConnectorSession session = new VacuumCleanerTest.ConnectorSession();
        final Table table = new Table("databaseName", "tableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("name", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("name", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final TypeManager typeManager = FunctionAndTypeManager.createTestFunctionAndTypeManager();
        final BiFunction<ConnectorSession, SchemaTableName, Optional<CatalogSchemaTableName>> tableRedirectionResolver = (val1, val2) -> {
            return null;
        };
        final MetadataProvider metadataProvider = null;

        // Run the test
        final ViewReaderUtil.ViewReader result = ViewReaderUtil.createViewReader(metastore, session, table, typeManager,
                tableRedirectionResolver, metadataProvider);

        // Verify the results
    }

    @Test
    public void testIsPrestoView1()
    {
        // Setup
        final Table table = new Table("databaseName", "tableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("name", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("name", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));

        // Run the test
        final boolean result = ViewReaderUtil.isPrestoView(table);
    }

    @Test
    public void testIsPrestoView2()
    {
        // Setup
        final Map<String, String> tableParameters = new HashMap<>();

        // Run the test
        final boolean result = ViewReaderUtil.isPrestoView(tableParameters);
    }

    @Test
    public void testIsHiveOrPrestoView1()
    {
        // Setup
        final Table table = new Table("databaseName", "tableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("name", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("name", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));

        // Run the test
        final boolean result = ViewReaderUtil.isHiveOrPrestoView(table);
    }

    @Test
    public void testIsHiveOrPrestoView2()
    {
        ViewReaderUtil.isHiveOrPrestoView("tableType");
    }

    @Test
    public void testCanDecodeView()
    {
        // Setup
        final Table table = new Table("databaseName", "tableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("name", HIVE_STRING, Optional.of("value"))),
                Arrays.asList(new Column("name", HIVE_STRING, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));

        // Run the test
        final boolean result = ViewReaderUtil.canDecodeView(table);
    }

    @Test
    public void testEncodeViewData()
    {
        // Setup
        final ConnectorViewDefinition definition = new ConnectorViewDefinition("originalSql", Optional.of("value"),
                Optional.of("value"), Arrays.asList(new ConnectorViewDefinition.ViewColumn("name",
                        new TypeSignature("base", TypeSignatureParameter.of(0L)))), Optional.of("value"), false);

        // Run the test
        final String result = ViewReaderUtil.encodeViewData(definition);
    }
}
