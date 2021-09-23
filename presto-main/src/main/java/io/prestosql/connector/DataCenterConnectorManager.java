/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.connector;

import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.spi.PrestoTransportException;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.HetuConstant.CONNECTION_URL;
import static io.prestosql.spi.HetuConstant.CONNECTION_USER;
import static io.prestosql.spi.HetuConstant.DATA_CENTER_CONNECTOR_NAME;

@ThreadSafe
public class DataCenterConnectorManager
{
    private static final Logger log = Logger.get(DataCenterUtility.class);

    private final ConnectorManager connectorManager;
    private final CatalogManager catalogManager;
    private final CatalogConnectorStore catalogConnectorStore;
    // HashSet for adding the catalog names in the query
    private final HashSet<String> loadedCatalogNames = new HashSet<>();

    @Inject
    public DataCenterConnectorManager(
            ConnectorManager connectorManager,
            CatalogManager catalogManager,
            CatalogConnectorStore catalogConnectorStore)
    {
        this.connectorManager = connectorManager;
        this.catalogManager = catalogManager;
        this.catalogConnectorStore = catalogConnectorStore;
    }

    /**
     * Hetu requires this method to add the DC sub catalog requested for.
     *
     * @param catalogName the catalog name
     * @param properties catalog properties
     * @param subCatalogName sub-catalog
     */
    private synchronized void addSubCatalog(String catalogName, String subCatalogName, Map<String, String> properties)
    {
        // Create a new catalog with the name <data-center>.<catalog-name>
        Map<String, String> newProperties = new HashMap<>(properties);
        String connectionUrl = properties.get(CONNECTION_URL);

        int indexOfDoubleSlash = connectionUrl.indexOf("//");
        int indexOfSlash = connectionUrl.indexOf('/', indexOfDoubleSlash + 2);
        if (indexOfSlash < 0) {
            // No slash in the url (For example jdbc:hetu://127.0.0.1:8080)
            connectionUrl = connectionUrl + '/' + subCatalogName;
        }
        else if (indexOfSlash == connectionUrl.length() - 1) {
            // There is a slash at the end of the url (For example jdbc:hetu://127.0.0.1:8080/)
            connectionUrl = connectionUrl + subCatalogName;
        }
        else {
            // There cannot be a / in the middle as in jdbc:hetu://10.122.252.106:8080/hive
            throw new IllegalArgumentException("DC Connector connection-url should contain only the host not sub-paths but found " + connectionUrl.substring(indexOfSlash));
        }

        if (properties.get(CONNECTION_URL).contains("jdbc")) {
            newProperties.put(CONNECTION_URL, connectionUrl);
        }

        CatalogName dcCatalog = new CatalogName(catalogName + "." + subCatalogName);

        // Add the catalog to hetu
        boolean isPresent = catalogManager.getCatalogs().stream()
                .map(Catalog::getCatalogName)
                .collect(Collectors.toSet())
                .contains(dcCatalog.getCatalogName());

        if (!isPresent) {
            ConnectorFactory factory = connectorManager.getConnectorFactories().get(DATA_CENTER_CONNECTOR_NAME);
            Connector newCatalogConnector = connectorManager.createConnector(dcCatalog, factory, newProperties);
            connectorManager.addCatalogConnector(dcCatalog, newCatalogConnector);
        }

        catalogConnectorStore.registerSubCatalog(catalogName, subCatalogName);
    }

    private synchronized void deleteSubCatalog(String catalogName, String subCatalogName)
    {
        catalogConnectorStore.dropSubCatalog(catalogName, subCatalogName);
        connectorManager.dropConnection(catalogName + "." + subCatalogName);
    }

    /**
     * Judge this catalog is a Data Center catalog or not.
     *
     * @param catalogName catalog name.
     * @return true if it is a Data Center catalog, else return false.
     */
    public boolean isDCCatalog(String catalogName)
    {
        return catalogConnectorStore.hasDCCatalogName(catalogName);
    }

    /**
     * Hetu requires this method to delete the DC catalog requested for.
     *
     * @param catalogName the sub catalog name
     */
    public synchronized void dropDCConnection(String catalogName)
    {
        Set<String> subCatalogNames = catalogConnectorStore.getSubCatalogs(catalogName);
        for (String subCatalogName : subCatalogNames) {
            deleteSubCatalog(catalogName, subCatalogName);
        }
        catalogConnectorStore.removeCatalog(catalogName);
    }

    /**
     * Hetu DC requires the method to load DC subcatalog. This method is
     * a public wrapper method to loadDCCatalog
     *
     * @param qualifiedCatalogName the dc sub catalog name, like dataCenter.hive
     */
    public void loadDCCatalog(String qualifiedCatalogName)
    {
        if (!qualifiedCatalogName.contains(".")) {
            return;
        }
        boolean needUpdateConnectionIds = false;
        synchronized (this) {
            String catalogName = qualifiedCatalogName.substring(0, qualifiedCatalogName.indexOf("."));
            if (catalogConnectorStore.hasDCCatalogName(catalogName)
                    && requireCatalogUpdate(catalogName)) {
                checkState(!connectorManager.getStopped().get(), "ConnectorManager is stopped");

                // get catalogs from remote data center.
                Connector catalogConnector = catalogConnectorStore.getCatalogConnector(catalogName);
                Map<String, String> properties = catalogConnectorStore.getCatalogProperties(catalogName);
                Collection<String> remoteSubCatalogNames = catalogConnector.getCatalogs(properties.get(CONNECTION_USER), properties);

                // check whether the catalog is in the remote data center.
                String subCatalogName = qualifiedCatalogName.substring(qualifiedCatalogName.indexOf(".") + 1);
                if (remoteSubCatalogNames.contains(subCatalogName)) {
                    // The catalog has not been loaded on the node.
                    if (!catalogConnectorStore.containsSubCatalog(catalogName, subCatalogName)) {
                        addSubCatalog(catalogName, subCatalogName, properties);
                        needUpdateConnectionIds = true;
                    }
                }
                else {
                    // The catalog does not exist on the remote end. Delete it.
                    deleteSubCatalog(catalogName, subCatalogName);
                    needUpdateConnectionIds = true;
                }
            }
        }
        if (needUpdateConnectionIds) {
            connectorManager.updateConnectorIds();
        }
    }

    /**
     * Hetu DC requires the method to load all the DC subcatalog. The method is called
     * while execution of 'show catalogs' query
     */
    public void loadAllDCCatalogs()
    {
        boolean needUpdateConnectionIds = false;
        synchronized (this) {
            List<String> dataCenterNames = catalogConnectorStore.getDataCenterNames();
            for (String dataCenterName : dataCenterNames) {
                try {
                    Connector catalogConnector = catalogConnectorStore.getCatalogConnector(dataCenterName);
                    Map<String, String> properties = catalogConnectorStore.getCatalogProperties(dataCenterName);
                    Set<String> remoteSubCatalogs = Sets.newHashSet(catalogConnector.getCatalogs(properties.get(CONNECTION_USER), properties));
                    // availableCatalogs is the List of all dc sub catalogs stored in Hetu
                    Set<String> availableCatalogs = catalogConnectorStore.getSubCatalogs(dataCenterName);

                    // This loop traverses through each remote sub catalog and add the remote sub catalogs
                    // which is not available in Hetu
                    for (String subCatalog : remoteSubCatalogs) {
                        if (!catalogConnectorStore.containsSubCatalog(dataCenterName, subCatalog)) {
                            addSubCatalog(dataCenterName, subCatalog, properties);
                            needUpdateConnectionIds = true;
                        }
                    }

                    // This loop traverses through each available catalog and deletes the sub catalog which
                    // is not available in remote Hetu dc
                    for (String availableCatalog : availableCatalogs) {
                        if (!remoteSubCatalogs.contains(availableCatalog)) {
                            deleteSubCatalog(dataCenterName, availableCatalog);
                            needUpdateConnectionIds = true;
                        }
                    }
                }
                catch (PrestoTransportException ignore) {
                    log.warn(ignore, "Load the catalogs of data center %s failed.", dataCenterName);
                }
            }
        }
        if (needUpdateConnectionIds) {
            connectorManager.updateConnectorIds();
        }
    }

    /**
     * Hetu DC requires the method to return if the catalog needs to be
     * updated based on the config threshold for dc catalog update
     *
     * @param catalogName the catalogName
     */
    private boolean requireCatalogUpdate(String catalogName)
    {
        if (!loadedCatalogNames.contains(catalogName)) {
            loadedCatalogNames.add(catalogName);
            return true;
        }

        boolean requiredCatalogUpdate = true;
        Long updateThreshold = catalogConnectorStore.getDCCatalogUpdateThreshold(catalogName);
        Long catalogUpdatedTime = catalogConnectorStore.getCatalogUpdatedTime(catalogName);
        if (catalogUpdatedTime != null) {
            if (System.currentTimeMillis() - catalogUpdatedTime < updateThreshold) {
                requiredCatalogUpdate = false;
            }
            else {
                catalogConnectorStore.registerCatalogUpdateTime(catalogName, Long.valueOf(System.currentTimeMillis()));
            }
        }
        else {
            catalogConnectorStore.registerCatalogUpdateTime(catalogName, Long.valueOf(System.currentTimeMillis()));
        }
        return requiredCatalogUpdate;
    }
}
