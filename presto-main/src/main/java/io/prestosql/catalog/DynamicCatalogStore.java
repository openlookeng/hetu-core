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
import io.prestosql.connector.CatalogName;
import io.prestosql.connector.ConnectorManager;
import io.prestosql.connector.DataCenterConnectorManager;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.spi.PrestoException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.util.PropertiesUtil.loadProperties;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class DynamicCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final DataCenterConnectorManager dataCenterConnectorManager;
    private final CatalogManager catalogManager;
    private final ServiceSelectorManager selectorManager;
    private final InternalNodeManager nodeManager;
    private final ExecutorService executorService;
    private final Duration scanInterval;
    private final DynamicCatalogConfig dynamicCatalogConfig;

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager,
            DataCenterConnectorManager dataCenterConnectorManager,
            DynamicCatalogConfig dynamicCatalogConfig,
            CatalogManager catalogManager,
            InternalNodeManager nodeManager,
            ServiceSelectorManager selectorManager)
    {
        this.connectorManager = connectorManager;
        this.dataCenterConnectorManager = dataCenterConnectorManager;
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.dynamicCatalogConfig = dynamicCatalogConfig;
        this.executorService = newFixedThreadPool(1, daemonThreadsNamed("dynamic-catalog-wait-%s"));
        this.scanInterval = dynamicCatalogConfig.getCatalogScannerInterval();
        this.selectorManager = selectorManager;
    }

    public synchronized Set<String> listCatalogNames(CatalogStore catalogStore)
            throws IOException
    {
        return ImmutableSet.copyOf(catalogStore.listCatalogNames());
    }

    public synchronized String getCatalogVersion(CatalogStore catalogStore, String catalogName)
            throws IOException
    {
        return catalogStore.getCatalogInformation(catalogName).getVersion();
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
            Map<String, String> properties = new HashMap<>(loadProperties(propertiesFile));
            properties.remove("connector.name");
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
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Try to load catalog failed, check your configuration", ex);
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

    private void updateLocalCatalog(CatalogStore localCatalogStore, CatalogInfo catalogInfo, CatalogFileInputStream configFiles)
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
    public synchronized void loadCatalog(CatalogStore localCatalogStore, CatalogStore shareCatalogStore, String catalogName)
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
    public synchronized void unloadCatalog(CatalogStore localCatalogStore, String catalogName)
    {
        unloadLocalCatalog(localCatalogStore, catalogName);
    }

    /**
     * reload catalog from share file system.
     *
     * @param catalogName catalog name
     */
    public synchronized void reloadCatalog(CatalogStore localCatalogStore, CatalogStore shareCatalogStore, String catalogName)
    {
        unloadCatalog(localCatalogStore, catalogName);
        loadCatalog(localCatalogStore, shareCatalogStore, catalogName);
    }

    /**
     * load catalog by specified config files, if load catalog success, then store the config files
     * to share file system.
     *
     * @param configFiles specified config files of catalog.
     */
    public synchronized void loadCatalogAndCreateShareFiles(CatalogStore localCatalogStore, CatalogStore shareCatalogStore, CatalogInfo catalogInfo, CatalogFileInputStream configFiles)
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

            log.info("-- Added catalog [%s] --", catalogName);
        }
        catch (IOException | PrestoException ex) {
            // if it has any exception, rollback
            log.error(ex, "-- Try to load catalog [%s] failed --", catalogName);
            unloadCatalog(localCatalogStore, catalogName);
            throw new PrestoException(GENERIC_USER_ERROR, ex.getMessage(), ex);
        }
    }

    /**
     * Delete catalog files from share file system.
     *
     * @param catalogName catalog name
     */
    public synchronized void deleteCatalogShareFiles(CatalogStore shareCatalogStore, String catalogName)
    {
        try {
            shareCatalogStore.deleteCatalog(catalogName, false);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Delete catalog failed.", e);
        }
    }

    private boolean isDeletedForAllNodes(String catalogName)
    {
        // default refresh time is 10s
        selectorManager.forceRefresh();
        // default refresh time is 10s
        nodeManager.refreshNodes();
        InternalNode currentNode = nodeManager.getCurrentNode();
        Set<InternalNode> nodes = nodeManager.getAllConnectorNodes(new CatalogName(catalogName));
        return (nodes.size() == 1 && nodes.contains(currentNode)) || // Current node has update catalog, so we just wait other nodes has deleted catalog.
                nodes.isEmpty(); // In the dc connector, when load dc catalog, it just load properties, sub-catalogs are not loaded.
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
            long startTime = System.currentTimeMillis();
            future.get((long) scanInterval.getValue() * 3, scanInterval.getUnit());
            long endTime = System.currentTimeMillis();
            log.info("Time required for deleting catalogs from all nodes : %dms", endTime - startTime);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            future.cancel(true);
            Set<InternalNode> nodes = nodeManager.getAllConnectorNodes(new CatalogName(catalogName));
            List<String> nodeIds = nodes.stream().map(InternalNode::getNodeIdentifier).collect(Collectors.toList());
            log.error("Wait all nodes unload catalog, but some nodes " + nodeIds + " timeout");
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Wait all nodes unload catalog, but some nodes timeout");
        }
    }

    private void updateRemoteCatalog(CatalogStore shareCatalogStore, CatalogInfo catalogInfo, CatalogFileInputStream configFiles)
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
    }

    public synchronized void updateCatalogAndShareFiles(CatalogStore localCatalogStore, CatalogStore shareCatalogStore, CatalogInfo catalogInfo, CatalogFileInputStream configFiles)
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

        log.info("-- Try to updating catalog [%s] to version [%d] --", catalogName, catalogInfo.getVersion());
        previousCatalogFiles.mark();
        try (CatalogFileInputStream newCatalogFiles = builder
                .putAll(previousCatalogFiles)
                .putAll(configFiles)
                .build()) {
            // update local catalog.
            //      config files need read again when been stored to share file system,
            //      so we need mark the input stream. After updated, reset the input streams.
            newCatalogFiles.mark();
            updateLocalCatalog(localCatalogStore, catalogInfo, newCatalogFiles);
            newCatalogFiles.reset();

            // update remote catalog.
            updateRemoteCatalog(shareCatalogStore, catalogInfo, newCatalogFiles);
        }
        catch (PrestoException | IOException ex) {
            // update failed, rollback.
            try {
                previousCatalogFiles.reset();
                shareCatalogStore.createCatalog(previousCatalogInfo, previousCatalogFiles);
            }
            catch (IOException ignore) {
                log.error(ignore, "Backup previous catalog information and files failed");
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

    public Lock getCatalogLock(CatalogStore shareCatalogStore, String catalogName)
            throws IOException
    {
        return shareCatalogStore.getCatalogLock(catalogName);
    }

    public void releaseCatalogLock(CatalogStore shareCatalogStore, String catalogName)
    {
        shareCatalogStore.releaseCatalogLock(catalogName);
    }
}
