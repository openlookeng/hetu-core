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
package io.hetu.core.plugin.mpp;

import com.google.common.cache.CacheLoader;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import io.airlift.event.client.EventClient;
import io.hetu.core.plugin.mpp.scheduler.Scheduler;
import io.prestosql.orc.BloomFilterCacheStatsLister;
import io.prestosql.orc.FileTailCacheStatsLister;
import io.prestosql.orc.RowDataCacheStatsLister;
import io.prestosql.orc.RowIndexCacheStatsLister;
import io.prestosql.orc.StripeFooterCacheStatsLister;
import io.prestosql.plugin.hive.CachingDirectoryLister;
import io.prestosql.plugin.hive.CoercionPolicy;
import io.prestosql.plugin.hive.DirectoryLister;
import io.prestosql.plugin.hive.DynamicConfigurationProvider;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.GenericHiveRecordCursorProvider;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveAnalyzeProperties;
import io.prestosql.plugin.hive.HiveCoercionPolicy;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveEventClient;
import io.prestosql.plugin.hive.HiveFileWriterFactory;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.HiveLocationService;
import io.prestosql.plugin.hive.HiveMetadataFactory;
import io.prestosql.plugin.hive.HiveModule;
import io.prestosql.plugin.hive.HiveNodePartitioningProvider;
import io.prestosql.plugin.hive.HivePageSinkProvider;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HivePageSourceProvider;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.HiveRecordCursorProvider;
import io.prestosql.plugin.hive.HiveSelectivePageSourceFactory;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveTableProperties;
import io.prestosql.plugin.hive.HiveTransactionManager;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.HiveWriterStats;
import io.prestosql.plugin.hive.LocationService;
import io.prestosql.plugin.hive.NamenodeStats;
import io.prestosql.plugin.hive.OrcFileWriterConfig;
import io.prestosql.plugin.hive.OrcFileWriterFactory;
import io.prestosql.plugin.hive.ParquetFileWriterConfig;
import io.prestosql.plugin.hive.PartitionUpdate;
import io.prestosql.plugin.hive.RcFileFileWriterFactory;
import io.prestosql.plugin.hive.S3SelectRecordCursorProvider;
import io.prestosql.plugin.hive.TransactionalMetadata;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.plugin.hive.orc.OrcSelectivePageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory;
import io.prestosql.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.prestosql.plugin.hive.s3.PrestoS3ClientFactory;
import io.prestosql.plugin.hive.util.IndexCache;
import io.prestosql.plugin.hive.util.IndexCacheLoader;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;

import java.util.function.Supplier;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class MppModule
        extends HiveModule
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeTranslator.class).toInstance(new HiveTypeTranslator());
        binder.bind(CoercionPolicy.class).to(HiveCoercionPolicy.class).in(Scopes.SINGLETON);

        binder.bind(HdfsConfigurationInitializer.class).in(Scopes.SINGLETON);
        newSetBinder(binder, DynamicConfigurationProvider.class);
        binder.bind(HdfsConfiguration.class).to(HiveHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(HdfsEnvironment.class).in(Scopes.SINGLETON);
        binder.bind(DirectoryLister.class).to(CachingDirectoryLister.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(HiveConfig.class);

        binder.bind(HiveSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(HiveAnalyzeProperties.class).in(Scopes.SINGLETON);

        binder.bind(NamenodeStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(NamenodeStats.class).withGeneratedName();

        binder.bind(PrestoS3ClientFactory.class).in(Scopes.SINGLETON);

        binder.bind(CachingDirectoryLister.class).in(Scopes.SINGLETON);
        newExporter(binder).export(CachingDirectoryLister.class).withGeneratedName();

        Multibinder<HiveRecordCursorProvider> recordCursorProviderBinder = newSetBinder(binder, HiveRecordCursorProvider.class);
        recordCursorProviderBinder.addBinding().to(S3SelectRecordCursorProvider.class).in(Scopes.SINGLETON);
        recordCursorProviderBinder.addBinding().to(GenericHiveRecordCursorProvider.class).in(Scopes.SINGLETON);

        binder.bind(HiveWriterStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HiveWriterStats.class).withGeneratedName();

        newSetBinder(binder, EventClient.class).addBinding().to(HiveEventClient.class).in(Scopes.SINGLETON);
        binder.bind(HivePartitionManager.class).in(Scopes.SINGLETON);
        binder.bind(LocationService.class).to(HiveLocationService.class).in(Scopes.SINGLETON);
        binder.bind(HiveMetadataFactory.class).to(MppMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(new TypeLiteral<Supplier<TransactionalMetadata>>() {}).to(HiveMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(HiveTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(MppSplitManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ConnectorSplitManager.class).as(generator -> generator.generatedNameOf(MppSplitManager.class));
        binder.bind(ConnectorPageSourceProvider.class).to(HivePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(HivePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(HiveNodePartitioningProvider.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(PartitionUpdate.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        Multibinder<HivePageSourceFactory> pageSourceFactoryBinder = newSetBinder(binder, HivePageSourceFactory.class);
        pageSourceFactoryBinder.addBinding().to(OrcPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(ParquetPageSourceFactory.class).in(Scopes.SINGLETON);
        pageSourceFactoryBinder.addBinding().to(RcFilePageSourceFactory.class).in(Scopes.SINGLETON);

        Multibinder<HiveSelectivePageSourceFactory> selectivePageSourceFactoryBinder = newSetBinder(binder, HiveSelectivePageSourceFactory.class);
        selectivePageSourceFactoryBinder.addBinding().to(OrcSelectivePageSourceFactory.class).in(Scopes.SINGLETON);

        Multibinder<HiveFileWriterFactory> fileWriterFactoryBinder = newSetBinder(binder, HiveFileWriterFactory.class);
        binder.bind(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(OrcFileWriterFactory.class).withGeneratedName();
        configBinder(binder).bindConfig(OrcFileWriterConfig.class);
        fileWriterFactoryBinder.addBinding().to(OrcFileWriterFactory.class).in(Scopes.SINGLETON);
        fileWriterFactoryBinder.addBinding().to(RcFileFileWriterFactory.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ParquetFileWriterConfig.class);

        binder.bind(CacheLoader.class).to(IndexCacheLoader.class).in(Scopes.SINGLETON);
        binder.bind(IndexCache.class).in(Scopes.SINGLETON);

        binder.bind(FileTailCacheStatsLister.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileTailCacheStatsLister.class).withGeneratedName();
        binder.bind(StripeFooterCacheStatsLister.class).in(Scopes.SINGLETON);
        newExporter(binder).export(StripeFooterCacheStatsLister.class).withGeneratedName();
        binder.bind(RowIndexCacheStatsLister.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RowIndexCacheStatsLister.class).withGeneratedName();
        binder.bind(BloomFilterCacheStatsLister.class).in(Scopes.SINGLETON);
        newExporter(binder).export(BloomFilterCacheStatsLister.class).withGeneratedName();
        binder.bind(RowDataCacheStatsLister.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RowDataCacheStatsLister.class).withGeneratedName();

        configBinder(binder).bindConfig(MppConfig.class);
        binder.bind(Scheduler.class).in(Scopes.SINGLETON);
    }
}
