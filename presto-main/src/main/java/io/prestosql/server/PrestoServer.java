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
package io.prestosql.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.event.client.JsonEventModule;
import io.airlift.event.client.http.HttpEventModule;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxHttpModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.prestosql.catalog.DynamicCatalogScanner;
import io.prestosql.catalog.DynamicCatalogStore;
import io.prestosql.discovery.HetuDiscoveryModule;
import io.prestosql.dynamicfilter.CrossRegionDynamicFilterListener;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.dynamicfilter.DynamicFilterListener;
import io.prestosql.eventlistener.EventListenerManager;
import io.prestosql.eventlistener.EventListenerModule;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.execution.warnings.WarningCollectorModule;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.httpserver.HetuHttpServerInfo;
import io.prestosql.httpserver.HetuHttpServerModule;
import io.prestosql.jmx.HetuJmxModule;
import io.prestosql.metadata.StaticCatalogStore;
import io.prestosql.metadata.StaticFunctionNamespaceStore;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.protocol.SmileModule;
import io.prestosql.security.AccessControlManager;
import io.prestosql.security.AccessControlModule;
import io.prestosql.security.GroupProviderManager;
import io.prestosql.security.PasswordSecurityModule;
import io.prestosql.seedstore.SeedStoreManager;
import io.prestosql.server.security.PasswordAuthenticatorManager;
import io.prestosql.server.security.ServerSecurityModule;
import io.prestosql.snapshot.SnapshotUtils;
import io.prestosql.spi.seedstore.SeedStoreSubType;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.statestore.StateStoreLauncher;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.statestore.listener.StateStoreListenerManager;
import io.prestosql.utils.HetuConfig;
import org.weakref.jmx.guice.MBeanModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.prestosql.server.PrestoSystemRequirements.verifyJvmRequirements;
import static io.prestosql.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;
import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTERS;
import static io.prestosql.utils.DynamicFilterUtils.MERGED_DYNAMIC_FILTERS;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.util.Objects.requireNonNull;

public class PrestoServer
        implements Runnable
{
    public static void main(String[] args)
    {
        new PrestoServer().run();
    }

    private final SqlParserOptions sqlParserOptions;

    public PrestoServer()
    {
        this(new SqlParserOptions());
    }

    public PrestoServer(SqlParserOptions sqlParserOptions)
    {
        this.sqlParserOptions = requireNonNull(sqlParserOptions, "sqlParserOptions is null");
    }

    @Override
    public void run()
    {
        verifyJvmRequirements();
        verifySystemTimeIsReasonable();

        Logger log = Logger.get(PrestoServer.class);

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new NodeModule(),
                Modules.override(new DiscoveryModule()).with(new HetuDiscoveryModule()),
                Modules.override(new HttpServerModule()).with(new HetuHttpServerModule()),
                new JsonModule(),
                new SmileModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new PrefixObjectNameGeneratorModule("io.prestosql"),
                new HetuJmxModule(),
                new JmxHttpModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                new JsonEventModule(),
                new HttpEventModule(),
                new ServerSecurityModule(),
                new AccessControlModule(),
                new PasswordSecurityModule(),
                new EventListenerModule(),
                new ServerMainModule(sqlParserOptions),
                new NodeStateChangeModule(),
                new WarningCollectorModule());

        modules.addAll(getAdditionalModules());

        Bootstrap app = new Bootstrap(modules.build());

        try {
            Injector injector = app.strictConfig().initialize();

            logLocation(log, "Working directory", Paths.get("."));
            logLocation(log, "Etc directory", Paths.get("etc"));

            injector.getInstance(PluginManager.class).loadPlugins();
            FileSystemClientManager fileSystemClientManager = injector.getInstance(FileSystemClientManager.class);
            fileSystemClientManager.loadFactoryConfigs();

            injector.getInstance(SeedStoreManager.class).loadSeedStore();
            if (injector.getInstance(SeedStoreManager.class).isSeedStoreOnYarnEnabled()) {
                addSeedOnYarnInformation(
                        injector.getInstance(ServerConfig.class),
                        injector.getInstance(SeedStoreManager.class),
                        (HetuHttpServerInfo) injector.getInstance(HttpServerInfo.class));
            }
            launchEmbeddedStateStore(injector.getInstance(HetuConfig.class), injector.getInstance(StateStoreLauncher.class));
            injector.getInstance(StateStoreProvider.class).loadStateStore();
            injector.getInstance(HetuMetaStoreManager.class).loadHetuMetastore(fileSystemClientManager); // relies on state-store

            injector.getInstance(HeuristicIndexerManager.class).buildIndexClient(); // relies on metastore
            injector.getInstance(StaticFunctionNamespaceStore.class).loadFunctionNamespaceManagers();
            injector.getInstance(StaticCatalogStore.class).loadCatalogs();
            injector.getInstance(DynamicCatalogStore.class).loadCatalogStores(fileSystemClientManager);
            injector.getInstance(DynamicCatalogScanner.class).start();
            injector.getInstance(SessionPropertyDefaults.class).loadConfigurationManager();
            injector.getInstance(ResourceGroupManager.class).loadConfigurationManager();
            injector.getInstance(AccessControlManager.class).loadSystemAccessControl();
            injector.getInstance(PasswordAuthenticatorManager.class).loadPasswordAuthenticator();
            injector.getInstance(EventListenerManager.class).loadConfiguredEventListener();
            injector.getInstance(GroupProviderManager.class).loadConfiguredGroupProvider();

            // preload index (on coordinator only)
            if (injector.getInstance(ServerConfig.class).isCoordinator()) {
                HeuristicIndexerManager heuristicIndexerManager = injector.getInstance(HeuristicIndexerManager.class);
                heuristicIndexerManager.preloadIndex();
                heuristicIndexerManager.initCache();
            }
            // register dynamic filter listener
            registerStateStoreListeners(
                    injector.getInstance(StateStoreListenerManager.class),
                    injector.getInstance(DynamicFilterCacheManager.class),
                    injector.getInstance(ServerConfig.class),
                    injector.getInstance(NodeSchedulerConfig.class));

            // Initialize snapshot Manager
            injector.getInstance(SnapshotUtils.class).initialize();

            injector.getInstance(Announcer.class).start();

            injector.getInstance(ServerInfoResource.class).startupComplete();

            log.info("======== SERVER STARTED ========");
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }
    }

    protected Iterable<? extends Module> getAdditionalModules()
    {
        return ImmutableList.of();
    }

    private static void addSeedOnYarnInformation(ServerConfig serverConfig,
                                                 SeedStoreManager seedStoreManager,
                                                 HetuHttpServerInfo httpServerInfo)
    {
        if (serverConfig == null || seedStoreManager == null || httpServerInfo == null) {
            return;
        }
        if (!serverConfig.isCoordinator()) {
            return;
        }
        String httpUri;
        if (httpServerInfo.getHttpExternalUri() != null) {
            httpUri = httpServerInfo.getHttpExternalUri().toString();
        }
        else if (httpServerInfo.getHttpsExternalUri() != null) {
            httpUri = httpServerInfo.getHttpsExternalUri().toString();
        }
        else {
            return;
        }
        try {
            seedStoreManager.addSeed(SeedStoreSubType.ON_YARN, httpUri, false);
        }
        catch (IOException e) {
            return;
        }
    }

    private static void launchEmbeddedStateStore(HetuConfig config, StateStoreLauncher launcher)
            throws Exception
    {
        // Only launch embedded state store when enabled
        if (config.isEmbeddedStateStoreEnabled()) {
            launcher.launchStateStore();
        }
    }

    private static void logLocation(Logger log, String name, Path path)
    {
        Path newPath = path;
        if (!Files.exists(newPath, NOFOLLOW_LINKS)) {
            log.info("%s: [does not exist]", name);
            return;
        }
        try {
            newPath = newPath.toAbsolutePath().toRealPath();
        }
        catch (IOException e) {
            log.info("%s: [not accessible]", name);
            return;
        }
        log.info("%s: %s", name, newPath);
    }

    private static void registerStateStoreListeners(
            StateStoreListenerManager stateStoreListenerManager,
            DynamicFilterCacheManager dynamicFilterCacheManager,
            ServerConfig serverConfig,
            NodeSchedulerConfig nodeSchedulerConfig)
    {
        if (serverConfig.isCoordinator() && !nodeSchedulerConfig.isIncludeCoordinator()) {
            return;
        }
        stateStoreListenerManager.addStateStoreListener(new DynamicFilterListener(dynamicFilterCacheManager), MERGED_DYNAMIC_FILTERS);
        stateStoreListenerManager.addStateStoreListener(new CrossRegionDynamicFilterListener(dynamicFilterCacheManager), CROSS_REGION_DYNAMIC_FILTERS);
    }
}
