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

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.EventModule;
import io.airlift.json.JsonModule;
import io.hetu.core.plugin.iceberg.catalog.IcebergCatalogModule;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.plugin.base.session.SessionPropertiesProvider;
import io.prestosql.plugin.hive.ConnectorObjectNameGeneratorModule;
import io.prestosql.plugin.hive.HiveHdfsModule;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.authentication.HdfsAuthenticationModule;
import io.prestosql.plugin.hive.authentication.HiveAuthenticationModule;
import io.prestosql.plugin.hive.azure.HiveAzureModule;
import io.prestosql.plugin.hive.gcs.HiveGcsModule;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.s3.HiveS3Module;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.TableProcedureMetadata;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.inject.Scopes.SINGLETON;

public final class InternalIcebergConnectorFactory
{
    private InternalIcebergConnectorFactory() {}

    public static Connector createConnector(
            String catalogName,
            Map<String, String> config,
            ConnectorContext context,
            Module module,
            Optional<HiveMetastore> metastore,
            Optional<FileIoProvider> fileIoProvider)
    {
        ClassLoader classLoader = InternalIcebergConnectorFactory.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new ConnectorObjectNameGeneratorModule(catalogName, "io.hetu.core.plugin.iceberg", "trino.plugin.iceberg"),
                    new JsonModule(),
                    new IcebergModule(),
                    new IcebergSecurityModule(),
                    new HiveAuthenticationModule(),
                    new IcebergCatalogModule(metastore),
                    new HiveHdfsModule(),
                    new HiveS3Module(),
                    new HiveGcsModule(),
                    new HiveAzureModule(),
                    new HdfsAuthenticationModule(),
                    new MBeanServerModule(),
                    fileIoProvider
                            .<Module>map(provider -> binder -> binder.bind(FileIoProvider.class).toInstance(provider))
                            .orElse(binder -> binder.bind(FileIoProvider.class).to(HdfsFileIoProvider.class).in(SINGLETON)),
                    binder -> {
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                    },
                    module);

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            IcebergTransactionManager transactionManager = injector.getInstance(IcebergTransactionManager.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider connectorPageSource = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            ConnectorNodePartitioningProvider connectorDistributionProvider = injector.getInstance(ConnectorNodePartitioningProvider.class);
            Set<SessionPropertiesProvider> sessionPropertiesProviders = injector.getInstance(Key.get(new TypeLiteral<Set<SessionPropertiesProvider>>() {}));
            IcebergTableProperties icebergTableProperties = injector.getInstance(IcebergTableProperties.class);
            Set<Procedure> procedures = injector.getInstance(Key.get(new TypeLiteral<Set<Procedure>>() {}));
            Set<TableProcedureMetadata> tableProcedures = injector.getInstance(Key.get(new TypeLiteral<Set<TableProcedureMetadata>>() {}));
            Optional<ConnectorAccessControl> accessControl = injector.getInstance(Key.get(new TypeLiteral<Optional<ConnectorAccessControl>>() {}));

            return new IcebergConnector(
                    lifeCycleManager,
                    transactionManager,
                    new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(connectorPageSource, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                    new ClassLoaderSafeNodePartitioningProvider(connectorDistributionProvider, classLoader),
                    sessionPropertiesProviders,
                    IcebergSchemaProperties.SCHEMA_PROPERTIES,
                    icebergTableProperties.getTableProperties(),
                    accessControl,
                    procedures,
                    tableProcedures);
        }
    }
}
