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

import io.prestosql.Session;
import io.prestosql.execution.TaskSource;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Use;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
        Optional<String> catalogName = Optional.empty();
        if (!catalogIdentifier.isPresent()) {
            catalogName = session.getCatalog();
        }
        else {
            catalogName = Optional.ofNullable(catalogIdentifier.get().getValue());
        }
        if (catalogName.isPresent()) {
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

    /**
     * Utility method to load all DC catalog from updateTask.
     *
     * @param metadata session
     * @param sources task source
     */
    public static void loadDCCatalogForUpdateTask(Metadata metadata, List<TaskSource> sources)
    {
        if (metadata instanceof MetadataManager) {
            // try to load dc catalog
            DataCenterConnectorManager dataCenterConnectorManager = ((MetadataManager) metadata).getDataCenterConnectorManager();
            Set<String> catalogNames = new HashSet<>();
            sources.stream().forEach(source -> {
                source.getSplits().stream()
                        .forEach(split ->
                        {
                            String catalogName = split.getSplit().getCatalogName().getCatalogName();
                            if (catalogName.contains(".")
                                    && dataCenterConnectorManager.isDCCatalog(catalogName.substring(0, catalogName.indexOf(".")))) {
                                catalogNames.add(catalogName);
                            }
                        });
            });
            catalogNames.stream()
                    .forEach(catalogName -> {
                        dataCenterConnectorManager.loadDCCatalog(catalogName);
                    });
        }
    }

    /**
     * Utility method to check whether a catalog is cross-dc catalog.
     *
     * @param metadata session
     * @param catalogName catalog name
     */
    public static boolean isDCCatalog(Metadata metadata, String name)
    {
        String catalogName = name;
        if (catalogName != null && catalogName.contains(".")) {
            catalogName = catalogName.substring(0, catalogName.indexOf("."));
        }
        MetadataManager metadataManager = (MetadataManager) metadata;
        DataCenterConnectorManager dataCenterConnectorManager = metadataManager.getDataCenterConnectorManager();
        if (dataCenterConnectorManager != null) {
            return dataCenterConnectorManager.isDCCatalog(catalogName);
        }
        return false;
    }
}
