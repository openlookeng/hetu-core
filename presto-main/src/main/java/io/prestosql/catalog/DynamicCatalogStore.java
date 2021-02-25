/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.discovery.client.ServiceSelectorManager;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.connector.DataCenterConnectorManager;
import io.prestosql.filesystem.FileSystemClientManager;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class DynamicCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private static final String CATALOG_NAME = "connector.name";

    private final ConnectorManager connectorManager;
    private final DataCenterConnectorManager dataCenterConnectorManager;
    private final CatalogManager catalogManager;
    private final ServiceSelectorManager selectorManager;
    private final InternalNodeManager nodeManager;
    private final ExecutorService executorService;
    private final Duration scanInterval;
    private final DynamicCatalogConfig dynamicCatalogConfig;
    private final CatalogStoreUtil catalogStoreUtil;
    private CatalogStore localCatalogStore;
    private CatalogStore shareCatalogStore;

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager,
            DataCenterConnectorManager dataCenterConnectorManager,
            DynamicCatalogConfig dynamicCatalogConfig,
            CatalogManager catalogManager,
            InternalNodeManager nodeManager,
            ServiceSelectorManager selectorManager,
            CatalogStoreUtil catalogStoreUtil)
    {
        this.connectorManager = connectorManager;
        this.dataCenterConnectorManager = dataCenterConnectorManager;
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.dynamicCatalogConfig = dynamicCatalogConfig;
        this.executorService = newFixedThreadPool(1, daemonThreadsNamed("dynamic-catalog-wait-%s"));
        this.scanInterval = dynamicCatalogConfig.getCatalogScannerInterval();
        this.selectorManager = selectorManager;
        this.catalogStoreUtil = catalogStoreUtil;
    }

    public void loadCatalogStores(FileSystemClientManager fileSystemClientManager)
            throws IOException
    {
        if (!dynamicCatalogConfig.isDynamicCatalogEnabled()) {
            return;
        }

        int maxCatalogFileSize = (int) dynamicCatalogConfig.getCatalogMaxFileSize().toBytes();
        String localConfigurationDir = dynamicCatalogConfig.getCatalogConfigurationDir();
        Properties properties = new Properties();
        properties.put("fs.client.type", "local");
        this.localCatalogStore = new LocalCatalogStore(localConfigurationDir,
                fileSystemClientManager.getFileSystemClient(properties, Paths.get(localConfigurationDir)),
                maxCatalogFileSize);

        String shareConfigurationDir = dynamicCatalogConfig.getCatalogShareConfigurationDir();
        this.shareCatalogStore = new ShareCatalogStore(shareConfigurationDir,
                fileSystemClientManager.getFileSystemClient(dynamicCatalogConfig.getShareFileSystemProfile(), Paths.get(shareConfigurationDir)),
                maxCatalogFileSize);
    }

    private CatalogStore getCatalogStore(CatalogStoreType type)
    {
        if (type == CatalogStoreType.LOCAL) {
            return localCatalogStore;
        }
        else {
            return shareCatalogStore;
        }
    }

    public synchronized Set<String> listCatalogNames(CatalogStoreType type)
            throws IOException
    {
        return ImmutableSet.copyOf(getCatalogStore(type).listCatalogNames());
    }

    public synchronized String getCatalogVersion(String catalogName, CatalogStoreType type)
            throws IOException
    {
        return getCatalogStore(type).getCatalogInformation(catalogName).getVersion();
    }

    private void loadLocalCatalog(CatalogStore localCatalogStore, CatalogInfo catalogInfo, CatalogFileInputStream configFiles)
    {
        String catalogName = catalogInfo.getCatalogName();
        log.info("-- Loading catalog [%s] --", catalogName);
        try {
            // store to local disk.
            localCatalogStore.createCatalog(catalogInfo, configFiles);

            // create connection, will load catalog from local disk to catalog manager.
            CatalogFilePath catalogPath = new CatalogFilePath(dynamicCatalogConfig.getCatalogConfigurationDir(), catalogName);
            File propertiesFile = catalogPath.getPropertiesPath().toFile();
            Map<String, String> properties = new HashMap<>(loadPropertiesFrom(propertiesFile.getPath()));
            catalogStoreUtil.decryptEncryptedProperties(catalogName, properties);
            properties.remove(CATALOG_NAME);
            connectorManager.createConnection(catalogName, catalogInfo.getConnectorName(), ImmutableMap.copyOf(properties));

            // add catalog to announcer, then each node knows current node has this catalog.
            connectorManager.updateConnectorIds();
            log.info("-- Loaded catalog [%s] --", catalogName);
        }
        catch (IOException | RuntimeException ex) {
            try {
                localCatalogStore.deleteCatalog(catalogName, false);
            }
            catch (IOException e) {
                log.error(e, "Delete catalog [%s] failed", catalogName);
            }
            String throwMessage = "Try to load catalog failed, check your configuration.";
            if (ex.getMessage() != null) {
                log.warn("%s cause by %s", throwMessage, ex.getMessage());
            }
            throw new PrestoException(GENERIC_INTERNAL_ERROR, throwMessage);
        }
    }

    private void unloadLocalCatalog(CatalogStore localCatalogStore, String catalogName)
    {
        log.info("-- Unloading catalog [%s] --", catalogName);
        if (dataCenterConnectorManager.isDCCatalog(catalogName)) {
            dataCenterConnectorManager.dropDCConnection(catalogName);
        }
        else {
            connectorManager.dropConnection(catalogName);
        }

        // remove catalog configuration files from local disk.
        try {
            localCatalogStore.deleteCatalog(catalogName, false);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Delete catalog failed.", e);
        }

        // remove catalog from announcer.
        connectorManager.updateConnectorIds();
        log.info("-- Removed catalog [%s] --", catalogName);
    }

    private void updateLocalCatalog(CatalogInfo catalogInfo, CatalogFileInputStream configFiles)
    {
        String catalogName = catalogInfo.getCatalogName();
        log.info("-- Updating catalog [%s] --", catalogName);
        // try to update local catalog. split two steps: unload and load
        try {
            unloadLocalCatalog(localCatalogStore, catalogName);
            loadLocalCatalog(localCatalogStore, catalogInfo, configFiles);
            log.info("-- Updated catalog [%s] --", catalogName);
        }
        catch (PrestoException ex) {
            log.error(ex, "-- Update catalog [%s] failed --", catalogName);
            // if update failed, we need delete the local catalog,
            // The Scanner loads the previous catalog version..
            unloadLocalCatalog(localCatalogStore, catalogName);
            throw ex;
        }
    }

    /**
     * load catalog from share file system, it will pull catalog related files from share file system
     * store to local disk, and load catalog to memory.
     * and finally, update the catalogs in announcer, let other coordinator and workers know that current
     * prestoServer haves this catalog.
     */
    public synchronized void loadCatalog(String catalogName)
    {
        // pull config files from share file system.
        try (CatalogFileInputStream configFiles = shareCatalogStore.getCatalogFiles(catalogName)) {
            if (catalogManager.getCatalog(catalogName).isPresent()) {
                log.warn("-- Catalog[%s] is already loaded, but catalog properties file is missing!!! --", catalogName);
                unloadLocalCatalog(localCatalogStore, catalogName);
            }

            // load catalog from local disk.
            CatalogInfo catalogInfo = shareCatalogStore.getCatalogInformation(catalogName);
            loadLocalCatalog(localCatalogStore, catalogInfo, configFiles);
        }
        catch (IOException | PrestoException ex) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, ex.getMessage(), ex);
        }
    }

    /**
     * unload catalog from memory
     */
    public synchronized void unloadCatalog(String catalogName)
    {
        unloadLocalCatalog(localCatalogStore, catalogName);
    }

    /**
     * reload catalog from share file system.
     *
     * @param catalogName catalog name
     */
    public synchronized void reloadCatalog(String catalogName)
    {
        unloadCatalog(catalogName);
        loadCatalog(catalogName);
    }

    /**
     * load catalog by specified config files, if load catalog success, then store the config files
     * to share file system.
     *
     * @param configFiles specified config files of catalog.
     */
    public synchronized void loadCatalogAndCreateShareFiles(CatalogInfo catalogInfo, CatalogFileInputStream configFiles)
    {
        String catalogName = catalogInfo.getCatalogName();
        try {
            log.info("-- Try to loading catalog [%s] --", catalogName);

            // store to local disk.
            //      config files need read again when been stored to share file system,
            //      so we need mark the input stream. After loaded, reset the input streams.
            configFiles.mark();
            loadLocalCatalog(localCatalogStore, catalogInfo, configFiles);
            configFiles.reset();

            // store the config files to share file system.
            shareCatalogStore.createCatalog(catalogInfo, configFiles);

            // wait for at least one node added catalog.
            waitForAtLeastOneNodeAddedCatalog(catalogName);

            log.info("-- Added catalog [%s] --", catalogName);
        }
        catch (IOException | PrestoException ex) {
            // if it has any exception, rollback
            log.error(ex, "-- Try to load catalog [%s] failed --", catalogName);
            unloadCatalog(catalogName);
            throw new PrestoException(GENERIC_USER_ERROR, ex.getMessage(), ex);
        }
    }

    /**
     * Delete catalog files from share file system.
     *
     * @param catalogName catalog name
     */
    public synchronized void deleteCatalogShareFiles(String catalogName)
    {
        try {
            shareCatalogStore.deleteCatalog(catalogName, false);
            connectorManager.dropConnection(catalogName);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Delete catalog failed.", e);
        }
    }

    private void refreshConnectorNodes()
    {
        // default refresh time is 10s
        selectorManager.forceRefresh();
        // default refresh time is 10s
        nodeManager.refreshNodes();
    }

    private boolean isAtLeastOneNodeAdded(String catalogName)
    {
        refreshConnectorNodes();
        Set<InternalNode> nodes = nodeManager.getActiveConnectorNodes(new CatalogName(catalogName));
        return nodes.size() >= 1;
    }

    private void waitForAtLeastOneNodeAddedCatalog(String catalogName)
    {
        Future<Integer> future = executorService.submit(new Callable<Integer>()
        {
            @Override
            public Integer call()
            {
                try {
                    while (!isAtLeastOneNodeAdded(catalogName)) {
                        TimeUnit.SECONDS.sleep(1);
                    }
                }
                catch (InterruptedException e) {
                    log.error(e, "Wait for at least one node loaded catalog, but sleep been interrupted");
                }
                return 0;
            }
        });

        try {
            future.get((long) scanInterval.getValue(), scanInterval.getUnit());
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            future.cancel(true);
            log.warn("Wait for at least one node loaded catalog, but timeout");
        }
    }

    private boolean isDeletedForAllNodes(String catalogName)
    {
        InternalNode currentNode = nodeManager.getCurrentNode();
        refreshConnectorNodes();
        Set<InternalNode> nodes = nodeManager.getAllConnectorNodes(new CatalogName(catalogName));
        return (nodes.size() == 1 && nodes.contains(currentNode)); // Current node has update catalog, so we just wait other nodes has deleted catalog.
    }

    private void waitForAllNodeDeletedCatalog(String catalogName)
    {
        Future<Integer> future = executorService.submit(new Callable<Integer>()
        {
            @Override
            public Integer call()
            {
                try {
                    while (!isDeletedForAllNodes(catalogName)) {
                        TimeUnit.SECONDS.sleep(1);
                    }
                }
                catch (InterruptedException e) {
                    log.error(e, "Wait for all node deleted catalog, but sleep been interrupted");
                }
                return 0;
            }
        });

        try {
            future.get((long) scanInterval.getValue() * 3, scanInterval.getUnit());
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            future.cancel(true);
            List<String> nodeIds = nodeManager.getAllConnectorNodes(new CatalogName(catalogName))
                    .stream()
                    .map(InternalNode::getNodeIdentifier)
                    .collect(Collectors.toList());
            log.error("Wait for all nodes unload catalog, but timeout, current nodes: " + nodeIds);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Wait all nodes unload catalog, but timeout");
        }
    }

    private void updateRemoteCatalog(CatalogInfo catalogInfo, CatalogFileInputStream configFiles)
    {
        String catalogName = catalogInfo.getCatalogName();
        // delete catalog files from share file system.
        try {
            shareCatalogStore.deleteCatalog(catalogName, false);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Delete catalog failed.", e);
        }
        // wait all nodes deleted catalog.
        waitForAllNodeDeletedCatalog(catalogName);

        // store new catalog files to share file system.
        try {
            shareCatalogStore.createCatalog(catalogInfo, configFiles);
        }
        catch (IOException ex) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Store new catalog files to share file system failed");
        }
        // wait for at least one node added catalog.
        waitForAtLeastOneNodeAddedCatalog(catalogName);
    }

    public synchronized void updateCatalogAndShareFiles(CatalogInfo catalogInfo, CatalogFileInputStream configFiles)
    {
        String catalogName = catalogInfo.getCatalogName();
        CatalogInfo previousCatalogInfo;
        CatalogFileInputStream previousCatalogFiles;

        // backup previous catalog information and files.
        try {
            previousCatalogInfo = shareCatalogStore.getCatalogInformation(catalogName);
            previousCatalogFiles = shareCatalogStore.getCatalogFiles(catalogName);
        }
        catch (IOException ex) {
            log.error(ex, "Backup previous catalog information and files failed");
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Backup previous catalog information and files failed");
        }

        // prepare new properties
        Map<String, String> newProperties = new HashMap<>(previousCatalogInfo.getProperties());
        newProperties.putAll(catalogInfo.getProperties());

        catalogInfo.setVersion(UUID.randomUUID().toString());
        catalogInfo.setProperties(newProperties);

        // prepare new files.
        CatalogFileInputStream.Builder builder = new CatalogFileInputStream.Builder((int) dynamicCatalogConfig.getCatalogMaxFileSize().toBytes());

        log.info("-- Try to updating catalog [%s] to version [%s] --", catalogName, catalogInfo.getVersion());
        previousCatalogFiles.mark();
        try (CatalogFileInputStream newCatalogFiles = builder
                .putAll(previousCatalogFiles)
                .putAll(configFiles)
                .build()) {
            // update local catalog.
            //      config files need read again when been stored to share file system,
            //      so we need mark the input stream. After updated, reset the input streams.
            newCatalogFiles.mark();
            updateLocalCatalog(catalogInfo, newCatalogFiles);
            newCatalogFiles.reset();

            // update remote catalog.
            updateRemoteCatalog(catalogInfo, newCatalogFiles);
        }
        catch (PrestoException | IOException ex) {
            // update failed, rollback.
            try {
                previousCatalogFiles.reset();
                shareCatalogStore.createCatalog(previousCatalogInfo, previousCatalogFiles);
            }
            catch (IOException ignore) {
                log.error(ignore, "Rollback previous catalog information and files failed");
            }
            throw new PrestoException(GENERIC_INTERNAL_ERROR, ex.getMessage(), ex);
        }
        finally {
            // close files.
            try {
                previousCatalogFiles.close();
            }
            catch (IOException ignore) {
                log.error(ignore, "Close previous catalog files failed");
            }
        }
    }

    public Lock getCatalogLock(String catalogName)
            throws IOException
    {
        return shareCatalogStore.getCatalogLock(catalogName);
    }

    public enum CatalogStoreType {
        LOCAL,
        SHARE
    }
}
