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
package io.hetu.core.plugin.iceberg;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.concurrent.BoundedExecutor;
import io.hetu.core.plugin.iceberg.metastore.thrift.BridgingHiveMetastoreFactory;
import io.hetu.core.plugin.iceberg.procedure.OptimizeTableProcedure;
import io.prestosql.metadata.TypeRegistry;
import io.prestosql.plugin.base.session.SessionPropertiesProvider;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.ForCachingHiveMetastore;
import io.prestosql.plugin.hive.ForCachingHiveMetastoreTableRefresh;
import io.prestosql.plugin.hive.ForHiveMetastore;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.metastore.HiveMetastoreFactory;
import io.prestosql.plugin.hive.metastore.MetastoreConfig;
import io.prestosql.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.prestosql.plugin.hive.orc.OrcReaderConfig;
import io.prestosql.plugin.hive.orc.OrcWriterConfig;
import io.prestosql.plugin.hive.parquet.ParquetReaderConfig;
import io.prestosql.plugin.hive.parquet.ParquetWriterConfig;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.TableProcedureMetadata;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.sql.analyzer.FeaturesConfig;

import javax.inject.Singleton;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(IcebergTransactionManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(HiveConfig.class);
        configBinder(binder).bindConfig(IcebergConfig.class);
        configBinder(binder).bindConfig(MetastoreConfig.class);

        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(IcebergSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(IcebergTableProperties.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).to(IcebergSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSourceProvider.class).setDefault().to(IcebergPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(IcebergPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(IcebergNodePartitioningProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(OrcReaderConfig.class);
        configBinder(binder).bindConfig(OrcWriterConfig.class);

        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);

        binder.bind(IcebergMetadataFactory.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(CommitTaskData.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        binder.bind(IcebergFileWriterFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IcebergFileWriterFactory.class).withGeneratedName();

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(RollbackToSnapshotProcedure.class).in(Scopes.SINGLETON);

        Multibinder<TableProcedureMetadata> tableProcedures = newSetBinder(binder, TableProcedureMetadata.class);
        tableProcedures.addBinding().toProvider(OptimizeTableProcedure.class).in(Scopes.SINGLETON);
        binder.bind(HiveMetastoreFactory.class).annotatedWith(RawHiveMetastoreFactory.class)
                .to(BridgingHiveMetastoreFactory.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(FeaturesConfig.class);
        binder.bind(TypeOperators.class).in(Scopes.SINGLETON);
        binder.bind(TypeRegistry.class).in(Scopes.SINGLETON);
    }

    @ForHiveMetastore
    @Singleton
    @Provides
    public ScheduledExecutorService createHiveMetadataClientServiceExecutor(CatalogName catalogName, HiveConfig hiveConfig)
    {
        return newScheduledThreadPool(
                hiveConfig.getMetastoreClientServiceThreads(),
                daemonThreadsNamed("hive-metastore-client-service-" + catalogName + "-%s"));
    }

    @ForCachingHiveMetastore
    @Singleton
    @Provides
    public Executor createCachingHiveMetastoreExecutor(CatalogName catalogName, HiveConfig hiveConfig)
    {
        return new BoundedExecutor(
                newCachedThreadPool(daemonThreadsNamed("hive-metastore-" + catalogName + "-%s")),
                (int) Math.max(hiveConfig.getMaxMetastoreRefreshThreads() * 0.9, 9));
    }

    @ForCachingHiveMetastoreTableRefresh
    @Singleton
    @Provides
    public Executor createCachingHiveMetastoreTableRefreshExecutor(CatalogName catalogName, HiveConfig hiveConfig)
    {
        return new BoundedExecutor(
                newCachedThreadPool(daemonThreadsNamed("hive-metastore-refresh-" + catalogName + "-%s")),
                (int) Math.max(hiveConfig.getMaxMetastoreRefreshThreads() * 0.1, 1));
    }
}
