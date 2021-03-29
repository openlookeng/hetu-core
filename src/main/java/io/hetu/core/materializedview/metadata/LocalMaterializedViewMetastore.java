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

package io.hetu.core.materializedview.metadata;

import io.airlift.log.Logger;
import io.hetu.core.materializedview.conf.MaterializedViewConfig;
import io.hetu.core.materializedview.utils.MaterializedViewConstants;
import io.hetu.core.materializedview.utils.MaterializedViewJsonUtils;
import io.hetu.core.materializedview.utils.MaterializedViewStatus;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.ViewNotFoundException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local Mv metastore
 *
 * @since 2020-03-26
 */
public class LocalMaterializedViewMetastore
        implements MaterializedViewMetastore
{
    private static final Logger LOG = Logger.get(LocalMaterializedViewMetastore.class);
    private static final int DEFAULT_SIZE = 16;
    private final String filePath;
    private final ConcurrentHashMap<String, Integer> schemaMap = new ConcurrentHashMap<>();
    private final Set<String> schemas = schemaMap.newKeySet();
    private final Map<String, Map<String, MaterializedView>> materialViews = new ConcurrentHashMap<>(DEFAULT_SIZE);

    /**
     * Constructor of local metastore
     *
     * @param config MV Config
     */
    public LocalMaterializedViewMetastore(MaterializedViewConfig config)
    {
        this.filePath = config.getMetastorePath();
    }

    @Override
    public void init()
    {
        try {
            schemas.clear();
            materialViews.clear();
            File file = new File(filePath);
            if (file.exists()) {
                MaterializedViewJsonUtils.loadMaterialViewFromJson(schemas, materialViews, Files.readAllBytes(Paths.get(filePath)));
            }
            else {
                if (file.createNewFile()) {
                    writeJsonToFile();
                }
                else {
                    throw new IOException("Create materialized view metadata file failed.");
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void addMaterializedView(String schema, MaterializedView materializedView)
    {
        synchronized (schemas) {
            synchronized (materialViews) {
                if (!schemas.contains(schema)) {
                    throw new SchemaNotFoundException(schema);
                }
                Map<String, MaterializedView> mvMap = materialViews.get(schema);
                mvMap.put(materializedView.getViewName(), materializedView);
                writeJsonToFile();
            }
        }
    }

    @Override
    public void dropMaterializedView(String schema, String view)
    {
        synchronized (schemas) {
            synchronized (materialViews) {
                if (!schemas.contains(schema)) {
                    throw new SchemaNotFoundException(schema);
                }
                Map<String, MaterializedView> mvMap = materialViews.get(schema);
                mvMap.remove(view);
                writeJsonToFile();
            }
        }
    }

    @Override
    public void addSchema(String schemaName)
    {
        synchronized (schemas) {
            synchronized (materialViews) {
                schemas.add(schemaName);
                materialViews.putIfAbsent(schemaName, new HashMap<>());
                writeJsonToFile();
            }
        }
    }

    @Override
    public void dropSchema(String schemaName)
    {
        synchronized (schemas) {
            synchronized (materialViews) {
                schemas.remove(schemaName);
                materialViews.remove(schemaName);
                writeJsonToFile();
            }
        }
    }

    @Override
    public Set<String> getSchemas()
    {
        return schemas;
    }

    @Override
    public List<String> getAllMaterializedView(String schemaName)
    {
        if (!schemas.contains(schemaName)) {
            return new ArrayList<>();
        }
        List<String> viewNames = new ArrayList<>();
        Map<String, MaterializedView> viewMap = materialViews.get(schemaName);
        for (Map.Entry<String, MaterializedView> entry : viewMap.entrySet()) {
            viewNames.add(entry.getKey());
        }
        return viewNames;
    }

    @Override
    public void setViewStatus(SchemaTableName viewName, MaterializedViewStatus status)
    {
        synchronized (schemas) {
            synchronized (materialViews) {
                if (!schemas.contains(viewName.getSchemaName())) {
                    throw new SchemaNotFoundException(viewName.getSchemaName());
                }
                MaterializedView view = materialViews.get(viewName.getSchemaName()).get(viewName.getTableName());
                if (view == null) {
                    throw new ViewNotFoundException(viewName);
                }
                else {
                    view.setStatus(status);
                    if (status == MaterializedViewStatus.ENABLE) {
                        view.setLastRefreshTime();
                    }
                    writeJsonToFile();
                }
            }
        }
    }

    @Override
    public MaterializedView getMaterializedView(SchemaTableName viewName)
    {
        if (!schemas.contains(viewName.getSchemaName())) {
            return null;
        }
        return materialViews.get(viewName.getSchemaName()).get(viewName.getTableName());
    }

    private void writeJsonToFile()
    {
        JSONObject jsonObject = MaterializedViewJsonUtils.materialViewsToJson(materialViews, schemas);
        PrintWriter out = null;
        String jsonStr = "";

        try {
            jsonStr = jsonObject.toString(MaterializedViewConstants.INDENTFACTOR_4);
        }
        catch (JSONException e) {
            LOG.error("WriteJsonToFile ERROR: Convert json to file failed for [%s]", e.getMessage());
        }

        File metaFile = new File(filePath);
        if (!metaFile.exists()) {
            try {
                if (!metaFile.createNewFile()) {
                    throw new IOException("Create File failed.");
                }
            }
            catch (IOException e) {
                LOG.error("WriteJsonToFile ERROR: Create metadata file failed for [%s]", e.getMessage());
            }
        }
        try {
            out = new PrintWriter(new FileWriter(filePath, false));
            out.write(jsonStr);
            out.flush();
        }
        catch (IOException e) {
            LOG.error("WriteJsonToFile ERROR: Write JSON To File Failed case by [%s]", e.getMessage());
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }
}
