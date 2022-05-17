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
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.cube.CubeManager;
import io.prestosql.eventlistener.EventListenerManager;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.failuredetector.FailureDetectorManager;
import io.prestosql.failuredetector.FailureDetectorPlugin;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metastore.HetuMetaStoreManager;
import io.prestosql.queryeditorui.store.connectors.ConnectorCache;
import io.prestosql.security.AccessControlManager;
import io.prestosql.security.GroupProviderManager;
import io.prestosql.seedstore.SeedStoreManager;
import io.prestosql.server.security.PasswordAuthenticatorManager;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.cube.CubeProvider;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import io.prestosql.spi.failuredetector.FailureRetryFactory;
import io.prestosql.spi.filesystem.HetuFileSystemClientFactory;
import io.prestosql.spi.function.FunctionNamespaceManagerFactory;
import io.prestosql.spi.function.SqlFunction;
import io.prestosql.spi.heuristicindex.IndexFactory;
import io.prestosql.spi.metastore.HetuMetaStoreFactory;
import io.prestosql.spi.resourcegroups.ResourceGroupConfigurationManagerFactory;
import io.prestosql.spi.security.GroupProviderFactory;
import io.prestosql.spi.security.PasswordAuthenticatorFactory;
import io.prestosql.spi.security.SystemAccessControlFactory;
import io.prestosql.spi.seedstore.SeedStoreFactory;
import io.prestosql.spi.session.SessionPropertyConfigurationManagerFactory;
import io.prestosql.spi.statestore.StateStoreBootstrapper;
import io.prestosql.spi.statestore.StateStoreFactory;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;
import io.prestosql.statestore.StateStoreLauncher;
import io.prestosql.statestore.StateStoreProvider;
import org.sonatype.aether.artifact.Artifact;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.metadata.FunctionExtractor.extractFunctions;
import static io.prestosql.server.PluginDiscovery.discoverPlugins;
import static io.prestosql.server.PluginDiscovery.writePluginServices;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PluginManager
{
    private static final ImmutableList<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("io.hetu.core.spi.")
            .add("io.prestosql.spi.")
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("io.airlift.units.")
            .add("org.openjdk.jol.")
            .add("io.prestosql.sql.tree.")
            .add("nova.hetu.omniruntime.vector.")
            .build();

    private static final Logger log = Logger.get(PluginManager.class);

    private static final String HIVE_FUNCTIONS_PLUGIN = "io.hetu.core.hive.HiveFunctionsPlugin";

    private final PluginManagerConfig config;
    private final ConnectorManager connectorManager;
    private final MetadataManager metadataManager;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final AccessControlManager accessControlManager;
    private final PasswordAuthenticatorManager passwordAuthenticatorManager;
    private final EventListenerManager eventListenerManager;
    private final GroupProviderManager groupProviderManager;
    private final CubeManager cubeManager;
    private final StateStoreProvider localStateStoreProvider;
    private final StateStoreLauncher stateStoreLauncher;
    private final SeedStoreManager seedStoreManager;
    private final HetuMetaStoreManager hetuMetaStoreManager;
    private final FileSystemClientManager fileSystemClientManager;
    private final FailureDetectorManager failureDetectorManager;
    private final HeuristicIndexerManager heuristicIndexerManager;
    private final SessionPropertyDefaults sessionPropertyDefaults;
    private final ArtifactResolver resolver;
    private final File installedPluginsDir;
    private final File externalFunctionsPluginsDir;
    private final List<String> plugins;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();

    @Inject
    public PluginManager(
            NodeInfo nodeInfo,
            PluginManagerConfig config,
            ConnectorManager connectorManager,
            MetadataManager metadataManager,
            ResourceGroupManager<?> resourceGroupManager,
            AccessControlManager accessControlManager,
            PasswordAuthenticatorManager passwordAuthenticatorManager,
            EventListenerManager eventListenerManager,
            GroupProviderManager groupProviderManager,
            CubeManager cubeManager,
            StateStoreProvider localStateStoreProvider, // StateStoreProvider
            StateStoreLauncher stateStoreLauncher,
            SessionPropertyDefaults sessionPropertyDefaults,
            SeedStoreManager seedStoreManager,
            FileSystemClientManager fileSystemClientManager,
            HetuMetaStoreManager hetuMetaStoreManager,
            HeuristicIndexerManager heuristicIndexerManager,
            FailureDetectorManager failureDetectorManager)
    {
        requireNonNull(nodeInfo, "nodeInfo is null");
        requireNonNull(config, "config is null");
        this.config = config;

        installedPluginsDir = config.getInstalledPluginsDir();
        externalFunctionsPluginsDir = config.getExternalFunctionsPluginsDir();
        if (config.getPlugins() == null) {
            this.plugins = ImmutableList.of();
        }
        else {
            this.plugins = ImmutableList.copyOf(config.getPlugins());
        }
        this.resolver = new ArtifactResolver(config.getMavenLocalRepository(), config.getMavenRemoteRepository());

        this.connectorManager = requireNonNull(connectorManager, "connectorManager is null");
        this.metadataManager = requireNonNull(metadataManager, "metadataManager is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.accessControlManager = requireNonNull(accessControlManager, "accessControlManager is null");
        this.passwordAuthenticatorManager = requireNonNull(passwordAuthenticatorManager, "passwordAuthenticatorManager is null");
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.groupProviderManager = requireNonNull(groupProviderManager, "groupProviderManager is null");
        this.cubeManager = requireNonNull(cubeManager, "cubeManager is null");
        // LocalStateProvider
        this.localStateStoreProvider = requireNonNull(localStateStoreProvider, "stateStoreManager is null");
        this.stateStoreLauncher = requireNonNull(stateStoreLauncher, "stateStoreLauncher is null");
        this.seedStoreManager = requireNonNull(seedStoreManager, "seedStoreManager is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");
        this.fileSystemClientManager = requireNonNull(fileSystemClientManager, "fileSystemClientManager is null");
        this.hetuMetaStoreManager = requireNonNull(hetuMetaStoreManager, "hetuMetaStoreManager is null");
        this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
        this.failureDetectorManager = requireNonNull(failureDetectorManager, "failureDetectorManager is null");
    }

    public void loadPlugins()
            throws Exception
    {
        if (!pluginsLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(installedPluginsDir)) {
            if (file.isDirectory()) {
                loadPlugin(file.getCanonicalPath());
            }
        }

        for (File file : listFiles(this.externalFunctionsPluginsDir)) {
            if (file.isDirectory()) {
                loadPlugin(file.getCanonicalPath(), true);
            }
        }

        for (String plugin : plugins) {
            loadPlugin(plugin);
        }

        metadataManager.verifyComparableOrderableContract();

        pluginsLoaded.set(true);
    }

    private void loadPlugin(String plugin)
            throws Exception
    {
        loadPlugin(plugin, false);
    }

    private void loadPlugin(String plugin, boolean onlyInstallFunctionsPlugin)
            throws Exception
    {
        log.info("-- Loading plugin %s --", plugin);
        URLClassLoader pluginClassLoader = buildClassLoader(plugin);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadPlugin(pluginClassLoader, onlyInstallFunctionsPlugin);
        }
        log.info("-- Finished loading plugin %s --", plugin);
    }

    private void loadPlugin(URLClassLoader pluginClassLoader, boolean onlyInstallFunctionsPlugin)
    {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> pluginSet = ImmutableList.copyOf(serviceLoader);
        checkState(!pluginSet.isEmpty(), "No service providers of type %s", Plugin.class.getName());
        for (Plugin plugin : pluginSet) {
            String name = plugin.getClass().getName();
            log.info("Installing %s", name);
            if (onlyInstallFunctionsPlugin) {
                installFunctionsPlugin(plugin);
                return;
            }
            if (HIVE_FUNCTIONS_PLUGIN.equals(name)) {
                plugin.setExternalFunctionsDir(this.config.getExternalFunctionsDir());
                plugin.setMaxFunctionRunningTimeEnable(this.config.getMaxFunctionRunningTimeEnable());
                plugin.setMaxFunctionRunningTimeInSec(this.config.getMaxFunctionRunningTimeInSec());
                plugin.setFunctionRunningThreadPoolSize(this.config.getFunctionRunningThreadPoolSize());
            }
            installPlugin(plugin);
        }
    }

    private void installFunctionsPlugin(Plugin plugin)
    {
        for (Class<?> functionClass : plugin.getFunctions()) {
            log.info("Registering functions from %s", functionClass.getName());
            metadataManager.getFunctionAndTypeManager().registerBuiltInFunctions(extractFunctions(functionClass));
        }

        for (Object dynamicHiveFunction : plugin.getDynamicHiveFunctions()) {
            log.info("Registering function %s", ((SqlFunction) dynamicHiveFunction).getSignature());
            metadataManager.getFunctionAndTypeManager().registerBuiltInFunctions(ImmutableList.of((SqlFunction) dynamicHiveFunction));
        }
    }

    public void installPlugin(Plugin plugin)
    {
        for (BlockEncoding blockEncoding : plugin.getBlockEncodings()) {
            log.info("Registering block encoding %s", blockEncoding.getName());
            metadataManager.getFunctionAndTypeManager().addBlockEncoding(blockEncoding);
        }

        for (Type type : plugin.getTypes()) {
            log.info("Registering type %s", type.getTypeSignature());
            metadataManager.getFunctionAndTypeManager().addType(type);
        }

        for (ParametricType parametricType : plugin.getParametricTypes()) {
            log.info("Registering parametric type %s", parametricType.getName());
            metadataManager.getFunctionAndTypeManager().addParametricType(parametricType);
        }

        for (ConnectorFactory connectorFactory : plugin.getConnectorFactories()) {
            log.info("Registering connector %s", connectorFactory.getName());
            connectorManager.addConnectorFactory(connectorFactory);
            ConnectorCache.addCatalogConfig(plugin, connectorFactory.getName());
        }

        for (SessionPropertyConfigurationManagerFactory sessionConfigFactory : plugin.getSessionPropertyConfigurationManagerFactories()) {
            log.info("Registering session property configuration manager %s", sessionConfigFactory.getName());
            sessionPropertyDefaults.addConfigurationManagerFactory(sessionConfigFactory);
        }

        for (FunctionNamespaceManagerFactory functionNamespaceManagerFactory : plugin.getFunctionNamespaceManagerFactories()) {
            log.info("Registering function namespace manager %s", functionNamespaceManagerFactory.getName());
            metadataManager.getFunctionAndTypeManager().addFunctionNamespaceFactory(functionNamespaceManagerFactory);
        }

        for (ResourceGroupConfigurationManagerFactory configurationManagerFactory : plugin.getResourceGroupConfigurationManagerFactories()) {
            log.info("Registering resource group configuration manager %s", configurationManagerFactory.getName());
            resourceGroupManager.addConfigurationManagerFactory(configurationManagerFactory);
        }

        for (SystemAccessControlFactory accessControlFactory : plugin.getSystemAccessControlFactories()) {
            log.info("Registering system access control %s", accessControlFactory.getName());
            accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        }

        for (PasswordAuthenticatorFactory authenticatorFactory : plugin.getPasswordAuthenticatorFactories()) {
            log.info("Registering password authenticator %s", authenticatorFactory.getName());
            passwordAuthenticatorManager.addPasswordAuthenticatorFactory(authenticatorFactory);
        }

        for (EventListenerFactory eventListenerFactory : plugin.getEventListenerFactories()) {
            log.info("Registering event listener %s", eventListenerFactory.getName());
            eventListenerManager.addEventListenerFactory(eventListenerFactory);
        }

        for (GroupProviderFactory groupProviderFactory : plugin.getGroupProviderFactories()) {
            log.info("Registering group provider %s", groupProviderFactory.getName());
            groupProviderManager.addGroupProviderFactory(groupProviderFactory);
        }

        // Install StateStorePlugin
        for (StateStoreBootstrapper bootstrapper : plugin.getStateStoreBootstrappers()) {
            log.info("Registering  state store bootstrapper");
            stateStoreLauncher.addStateStoreBootstrapper(bootstrapper);
        }
        for (StateStoreFactory stateStoreFactory : plugin.getStateStoreFactories()) {
            log.info("Registering state store %s", stateStoreFactory.getName());
            localStateStoreProvider.addStateStoreFactory(stateStoreFactory);
        }

        for (SeedStoreFactory seedStoreFactory : plugin.getSeedStoreFactories()) {
            log.info("Registering seed store %s", seedStoreFactory.getName());
            seedStoreManager.addSeedStoreFactory(seedStoreFactory);
        }

        for (CubeProvider cubeProvider : plugin.getCubeProviders()) {
            log.info("Registering cube provider %s", cubeProvider.getName());
            cubeManager.addCubeProvider(cubeProvider);
        }

        for (HetuFileSystemClientFactory fileSystemClientFactory : plugin.getFileSystemClientFactory()) {
            log.info("Registering file system provider %s", fileSystemClientFactory.getName());
            fileSystemClientManager.addFileSystemClientFactories(fileSystemClientFactory);
        }

        for (HetuMetaStoreFactory hetuMetaStoreFactory : plugin.getHetuMetaStoreFactories()) {
            log.info("Registering hetu metastore %s", hetuMetaStoreFactory.getName());
            hetuMetaStoreManager.addHetuMetaStoreFactory(hetuMetaStoreFactory);
        }

        for (IndexFactory indexFactory : plugin.getIndexFactories()) {
            log.info("Loading index factory");
            heuristicIndexerManager.loadIndexFactories(indexFactory);
        }

        // to-do: make failure detector as a plugin
        FailureDetectorPlugin fplugin = new FailureDetectorPlugin();
        for (FailureRetryFactory failureRetryFactory : fplugin.getFailureRetryFactory()) {
            log.info("Registering failure retry policy provider %s", failureRetryFactory.getName());
            FailureDetectorManager.addFailureRetryFactory(failureRetryFactory);
        }

        installFunctionsPlugin(plugin);
    }

    private URLClassLoader buildClassLoader(String plugin)
            throws Exception
    {
        File file = new File(plugin);
        if (file.isFile() && (file.getName().equals("pom.xml") || file.getName().endsWith(".pom"))) {
            return buildClassLoaderFromPom(file);
        }
        if (file.isDirectory()) {
            return buildClassLoaderFromDirectory(file);
        }
        return buildClassLoaderFromCoordinates(plugin);
    }

    private URLClassLoader buildClassLoaderFromPom(File pomFile)
            throws Exception
    {
        List<Artifact> artifacts = resolver.resolvePom(pomFile);
        URLClassLoader classLoader = createClassLoader(artifacts, pomFile.getPath());

        Artifact artifact = artifacts.get(0);
        Set<String> pluginSet = discoverPlugins(artifact, classLoader);
        if (!pluginSet.isEmpty()) {
            writePluginServices(pluginSet, artifact.getFile());
        }

        return classLoader;
    }

    private URLClassLoader buildClassLoaderFromDirectory(File dir)
            throws Exception
    {
        log.debug("Classpath for %s:", dir.getName());
        List<URL> urls = new ArrayList<>();
        for (File file : listFiles(dir)) {
            log.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader buildClassLoaderFromCoordinates(String coordinates)
            throws Exception
    {
        Artifact rootArtifact = new DefaultArtifact(coordinates);
        List<Artifact> artifacts = resolver.resolveArtifacts(rootArtifact);
        return createClassLoader(artifacts, rootArtifact.toString());
    }

    private URLClassLoader createClassLoader(List<Artifact> artifacts, String name)
            throws IOException
    {
        log.debug("Classpath for %s:", name);
        List<URL> urls = new ArrayList<>();
        for (Artifact artifact : sortedArtifacts(artifacts)) {
            if (artifact.getFile() == null) {
                throw new RuntimeException("Could not resolve artifact: " + artifact);
            }
            File file = artifact.getFile().getCanonicalFile();
            log.debug("    %s", file);
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader createClassLoader(List<URL> urls)
    {
        ClassLoader parent = getClass().getClassLoader();
        return new PluginClassLoader(urls, parent, SPI_PACKAGES);
    }

    private static List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                Arrays.sort(files);
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static List<Artifact> sortedArtifacts(List<Artifact> artifacts)
    {
        List<Artifact> list = new ArrayList<>(artifacts);
        Collections.sort(list, Ordering.natural().nullsLast().onResultOf(Artifact::getFile));
        return list;
    }
}
