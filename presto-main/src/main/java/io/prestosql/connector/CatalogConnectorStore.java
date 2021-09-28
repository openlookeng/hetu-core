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

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.Connector;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;

/**
 * Hetu DC requires the CatalogConnectorStore to store the Connector objects, Catalog properties and the Catalog updated times.
 * The class CatalogConnectorStore manipulates thread safe objects.
 */
@ThreadSafe
public class CatalogConnectorStore
{
    private final Map<String, ConnectorProperties> catalogConnectorProperties = new ConcurrentHashMap<>();
    private final Map<String, Long> catalogUpdatedTime = new ConcurrentHashMap<>();
    //The default catalog update threshold time is 0 in millis.
    private static final long DEFAULT_CATALOG_UPDATE_THRESHOLD_TIME = 0L;
    private static final String DC_CATALOG_UPDATE_THRESHOLD_PROPERTY_NAME = "hetu.dc.catalog.update.threshold";

    public CatalogConnectorStore() {}

    public void registerConnectorAndProperties(CatalogName dcCatalogName, Connector connector, Map<String, String> properties)
    {
        ConnectorProperties connectorProperties = new ConnectorProperties(connector, properties);
        checkState(catalogConnectorProperties.put(dcCatalogName.getCatalogName(), connectorProperties) == null, "dcCatalogName '%s' has already registered properties and Connector", dcCatalogName);
    }

    /**
     * Hetu DC requires the method to remove the registered dc catalog connector
     * @param catalogName the catalog name
     */
    public void removeCatalog(String catalogName)
    {
        catalogConnectorProperties.remove(catalogName);
    }

    /**
     * Hetu DC requires the method to return all the registered dc catalog names
     */
    public List<String> getDataCenterNames()
    {
        return ImmutableList.copyOf(catalogConnectorProperties.keySet());
    }

    /**
     * Hetu DC requires the method to return the registered dc catalog connector for
     * particular catalog name
     * @param catalogName the catalog name
     */
    public Connector getCatalogConnector(String catalogName)
    {
        return catalogConnectorProperties.get(catalogName).getConnector();
    }

    /**
     * Hetu DC requires the method to return the registered dc catalog properties for
     * particular catalog name
     *
     * @param catalogName the catalog name
     */
    public Map<String, String> getCatalogProperties(String catalogName)
    {
        return catalogConnectorProperties.get(catalogName).getProperties();
    }

    /**
     * Hetu DC requires the method to check whether the catalog name is registered or not
     * @param catalogName the catalog name
     */
    public boolean hasDCCatalogName(String catalogName)
    {
        return catalogConnectorProperties.containsKey(catalogName);
    }

    /**
     * Hetu DC requires the method to register DC sub catalog
     * @param dcCatalogName the DC connector name
     * @param subCatalogName the sub catalog name
     */
    public void registerSubCatalog(String dcCatalogName, String subCatalogName)
    {
        catalogConnectorProperties.get(dcCatalogName).registerSubCatalog(subCatalogName);
    }

    /**
     * Hetu DC requires the method to remove the existing DC sub catalog
     * @param subCatalogName the sub catalog name
     */
    public void dropSubCatalog(String dcCatalogName, String subCatalogName)
    {
        catalogConnectorProperties.get(dcCatalogName).dropSubCatalog(subCatalogName);
    }

    /**
     * Hetu DC requires the method to check if the DC sub catalog
     * already exists
     * @param dcCatalogName the dc catalog name
     * @param subCatalog the dc sub catalog name
     */
    public boolean containsSubCatalog(String dcCatalogName, String subCatalog)
    {
        return catalogConnectorProperties.get(dcCatalogName).getSubCatalogs().contains(subCatalog);
    }

    /**
     * Hetu DC requires the method to return all the existing DC sub catalogs as List<String>
     */
    public Set<String> getSubCatalogs(String dcCatalogName)
    {
        return catalogConnectorProperties.get(dcCatalogName).getSubCatalogs();
    }

    /**
     * Hetu DC requires the method to return the DC catalog update threshold
     * @param catalogName DC catalog name
     */
    public synchronized Long getDCCatalogUpdateThreshold(String catalogName)
    {
        Map<String, String> catalogProperties = catalogConnectorProperties.get(catalogName).getProperties();
        String dcCatalogUpdateTime = catalogProperties.get(DC_CATALOG_UPDATE_THRESHOLD_PROPERTY_NAME);
        if (dcCatalogUpdateTime != null) {
            return Duration.valueOf(dcCatalogUpdateTime).toMillis();
        }
        else {
            return DEFAULT_CATALOG_UPDATE_THRESHOLD_TIME;
        }
    }

    /**
     * Hetu DC requires the method to return the updated time for a catalog
     */
    public Long getCatalogUpdatedTime(String catalogName)
    {
        return catalogUpdatedTime.get(catalogName);
    }

    /**
     * Hetu DC requires the method to register the update time of a catalog
     */
    public void registerCatalogUpdateTime(String catalogName, Long timeInMillis)
    {
        catalogUpdatedTime.put(catalogName, timeInMillis);
    }

    @ThreadSafe
    private static class ConnectorProperties
    {
        private final Connector connector;
        private final Map<String, String> properties;
        private final Set<String> subCatalogs = ConcurrentHashMap.newKeySet();

        public ConnectorProperties(Connector connector, Map<String, String> properties)
        {
            this.connector = connector;
            this.properties = properties;
        }

        /**
         * Hetu DC requires the method to return the DC Connector
         */
        public Connector getConnector()
        {
            return connector;
        }

        /**
         * Hetu DC requires the method to return the properties of DC catalog
         */
        public Map<String, String> getProperties()
        {
            return properties;
        }

        /**
         * Hetu DC requires the method to register DC sub catalog
         * @param subCatalog the sub catalog name
         */
        public void registerSubCatalog(String subCatalog)
        {
            subCatalogs.add(subCatalog);
        }

        /**
         * Hetu DC requires the method to remove the existing DC sub catalog
         * @param subCatalog the sub catalog name
         */
        public void dropSubCatalog(String subCatalog)
        {
            subCatalogs.remove(subCatalog);
        }

        /**
         * Hetu DC requires the method to remove the existing DC sub catalog
         */
        public Set<String> getSubCatalogs()
        {
            return subCatalogs;
        }
    }
}
