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
package io.prestosql.plugin.hive.orc;

import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.prestosql.orc.OrcCacheProperties;
import io.prestosql.orc.OrcCacheStore;
import io.prestosql.orc.OrcFileTail;
import io.prestosql.orc.OrcReader;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.HivePageSourceProvider;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.VacuumCleanerTest;
import io.prestosql.plugin.hive.WriteIdInfo;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.coercions.HiveCoercer;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.ivy.util.EncrytedProperties;
import org.joda.time.DateTimeZone;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.mockito.MockitoAnnotations.initMocks;

public class OrcSelectivePageSourceFactoryTest
{
    @Mock
    private TypeManager mockTypeManager;
    @Mock
    private HiveConfig mockConfig;
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;
    @Mock
    private FileFormatDataSourceStats mockStats;
    @Mock
    private OrcCacheStore mockOrcCacheStore;

    private OrcSelectivePageSourceFactory orcSelectivePageSourceFactoryUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        orcSelectivePageSourceFactoryUnderTest = new OrcSelectivePageSourceFactory(mockTypeManager, mockConfig,
                mockHdfsEnvironment, mockStats, mockOrcCacheStore);
    }

    @Test
    public void testCreatePageSource() throws Exception
    {
        // Setup
        final Configuration configuration = new Configuration(false);
        final ConnectorSession session = new VacuumCleanerTest.ConnectorSession();
        final Path path = new Path("pathstring", "child");
        final Properties schema = new EncrytedProperties();
        final List<HiveColumnHandle> columns = Arrays.asList(
                new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false));
        final Map<Integer, String> prefilledValues = new HashMap<>();
        final TupleDomain<HiveColumnHandle> domainPredicate = TupleDomain.withColumnDomains(new HashMap<>());
        final Optional<List<TupleDomain<HiveColumnHandle>>> additionPredicates = Optional.of(
                Arrays.asList(TupleDomain.withColumnDomains(new HashMap<>())));
        final Optional<DeleteDeltaLocations> deleteDeltaLocations = Optional.of(
                new DeleteDeltaLocations("partitionLocation", Arrays.asList(new WriteIdInfo(0L, 0L, 0))));
        final Optional<List<IndexMetadata>> indexes = Optional.of(
                Arrays.asList(new IndexMetadata(null, "table", new String[]{"columns"}, "rootUri", "uri", 0L, 0L)));
        final List<HivePageSourceProvider.ColumnMapping> columnMappings = Arrays.asList(
                HivePageSourceProvider.ColumnMapping.interim(
                        new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                                new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                                HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false), 0));
        final Map<Integer, HiveCoercer> coercers = new HashMap<>();

        // Run the test
        final Optional<? extends ConnectorPageSource> result = orcSelectivePageSourceFactoryUnderTest.createPageSource(
                configuration, session, path, 0L, 0L, 0L, schema, columns, prefilledValues,
                Arrays.asList(0), domainPredicate, additionPredicates, deleteDeltaLocations, Optional.of(0L), indexes,
                false, columnMappings, coercers, 0L, Optional.empty());

        // Verify the results
    }

    @Test
    public void testCreateOrcPageSource()
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
        hiveConfig.setUseOrcColumnNames(false);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveConfig), ImmutableSet.of());
        final HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveConfig, new NoHdfsAuthentication());
        final ConnectorSession session = new VacuumCleanerTest.ConnectorSession();
        final Configuration configuration = new Configuration(false);
        final Path path = new Path("pathstring", "child");
        final List<HiveColumnHandle> columns = Arrays.asList(
                new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false));
        final Map<Integer, String> prefilledValues = new HashMap<>();
        final TupleDomain<HiveColumnHandle> domainPredicate = TupleDomain.withColumnDomains(new HashMap<>());
        final DateTimeZone hiveStorageTimeZone = DateTimeZone.forID("UTC");
        final TypeManager typeManager = null;
        final DataSize maxMergeDistance = new DataSize(0.0, DataSize.Unit.BYTE);
        final DataSize maxBufferSize = new DataSize(0.0, DataSize.Unit.BYTE);
        final DataSize streamBufferSize = new DataSize(0.0, DataSize.Unit.BYTE);
        final DataSize tinyStripeThreshold = new DataSize(0.0, DataSize.Unit.BYTE);
        final DataSize maxReadBlockSize = new DataSize(0.0, DataSize.Unit.BYTE);
        final FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        final Optional<DeleteDeltaLocations> deleteDeltaLocations = Optional.of(
                new DeleteDeltaLocations("partitionLocation", Arrays.asList(new WriteIdInfo(0L, 0L, 0))));
        final Optional<List<IndexMetadata>> indexes = Optional.of(
                Arrays.asList(new IndexMetadata(null, "table", new String[]{"columns"}, "rootUri", "uri", 0L, 0L)));
        final OrcCacheStore orcCacheStore = null;
        final OrcCacheProperties orcCacheProperties = new OrcCacheProperties(false, false, false, false, false);
        final List<TupleDomain<HiveColumnHandle>> disjunctDomains = Arrays.asList(
                TupleDomain.withColumnDomains(new HashMap<>()));
        final List<HivePageSourceProvider.ColumnMapping> columnMappings = Arrays.asList(
                HivePageSourceProvider.ColumnMapping.interim(
                        new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                                new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                                HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false), 0));
        final Map<Integer, HiveCoercer> coercers = new HashMap<>();

        // Run the test
        final OrcSelectivePageSource result = OrcSelectivePageSourceFactory.createOrcPageSource(hdfsEnvironment,
                session, configuration, path, 1L, 1L, 1L, columns, false, false, prefilledValues,
                Arrays.asList(1), domainPredicate, hiveStorageTimeZone, typeManager, maxMergeDistance, maxBufferSize,
                streamBufferSize, tinyStripeThreshold, maxReadBlockSize, false, false, stats, deleteDeltaLocations,
                Optional.of(1L), indexes, orcCacheStore, orcCacheProperties, disjunctDomains, Arrays.asList(1),
                columnMappings, coercers, 1L, Optional.empty());

        // Verify the results
    }

    @Test
    public void testVerifyAcidSchema() throws Exception
    {
        // Setup
        final OrcReader orcReader = new OrcReader(null, OrcFileTail.readFrom(null, Optional.empty()),
                new DataSize(0.0, DataSize.Unit.BYTE), new DataSize(0.0, DataSize.Unit.BYTE),
                new DataSize(0.0, DataSize.Unit.BYTE));
        final Path path = new Path("scheme", "authority", "path");

        // Run the test
        OrcSelectivePageSourceFactory.verifyAcidSchema(orcReader, path);

        // Verify the results
    }
}
