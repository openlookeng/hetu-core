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
package io.prestosql.connector;

import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Use;

import java.util.Optional;

public class DataCenterUtility
{
    private DataCenterUtility() {}

    /**
     * Utility method to load DC catalog from Usetask.
     *
     * @param statement statement
     * @param metadata metadata
     * @param session session
     */
    public static void loadDCCatalogForUseTask(Use statement, Metadata metadata, Session session)
    {
        Optional<Identifier> catalogIdentifier = statement.getCatalog();
        if (catalogIdentifier.isPresent()) {
            Optional<String> catalogName = Optional.ofNullable(catalogIdentifier.get().getValue());
            if (catalogName.isPresent()) {
                MetadataManager metadataManager = (MetadataManager) metadata;
                DataCenterConnectorManager dataCenterConnectorManager = metadataManager.getDataCenterConnectorManager();
                if (dataCenterConnectorManager != null) {
                    dataCenterConnectorManager.loadDCCatalog(catalogName.get());
                }
            }
        }
    }

    /**
     * Utility method to load DC catalog from query flow.
     *
     * @param session session
     * @param metadata metadata
     */
    public static void loadDCCatalogForQueryFlow(Session session, Metadata metadata, String catalogName)
    {
        if (!catalogName.contains(".")) {
            return;
        }
        MetadataManager metadataManager = (MetadataManager) metadata;
        DataCenterConnectorManager dataCenterConnectorManager = metadataManager.getDataCenterConnectorManager();
        if (dataCenterConnectorManager != null) {
            dataCenterConnectorManager.loadDCCatalog(catalogName);
        }
    }

    /**
     * Utility method to load all DC catalog from ShowQueriesRewrite.
     *
     * @param session session
     * @param metadata metadata
     */
    public static void loadDCCatalogsForShowQueries(Metadata metadata, Session session)
    {
        MetadataManager metadataManager = (MetadataManager) metadata;
        DataCenterConnectorManager dataCenterConnectorManager = metadataManager.getDataCenterConnectorManager();
        if (dataCenterConnectorManager != null) {
            dataCenterConnectorManager.loadAllDCCatalogs();
        }
    }

    public static boolean isDCCatalog(Metadata metadata, String catalogName)
    {
        MetadataManager metadataManager = (MetadataManager) metadata;
        DataCenterConnectorManager dataCenterConnectorManager = metadataManager.getDataCenterConnectorManager();
        if (dataCenterConnectorManager != null) {
            return dataCenterConnectorManager.isDCCatalog(catalogName);
        }
        return false;
    }
}
