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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.hetu.core.materializedview.connector.MaterializedViewColumnHandle;
import io.hetu.core.materializedview.utils.MaterializedViewDateUtils;
import io.hetu.core.materializedview.utils.MaterializedViewStatus;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Date;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Material view
 *
 * @since 2020-03-25
 */
public class MaterializedView
{
    private final String viewName;
    private final String viewSql;
    private final String fullSql;
    private final String dataCatalog; // the catalog which store materialized view data
    private final String dataSchema; // the schema which store materialized view data
    private final String metaCatalog; // the catalog which store materialized view metadata
    private final String currentCatalog; // the catalog which current session used
    private final String currentSchema; // the schema which current session used
    private final List<MaterializedViewColumnHandle> columns;
    private final String owner;
    private final boolean runAsInvoker;
    private Date lastRefreshTime;
    private MaterializedViewStatus status;

    /**
     * constructor of material view
     *
     * @param viewName view name
     * @param viewSql view sql
     * @param fullSql full sql
     * @param dataCatalog data catalog
     * @param dataSchema data schema
     * @param metaCatalog metadata catalog
     * @param currentCatalog current session catalog
     * @param currentSchema current session schema
     * @param columns columns
     * @param owner owner
     * @param runAsInvoker runAsInvoker
     * @param lastRefreshTime last refresh time
     * @param status status
     */
    @JsonCreator
    // CHECKSTYLE:OFF:ParameterNumber
    public MaterializedView(
            @JsonProperty("viewName") String viewName,
            @JsonProperty("viewSql") String viewSql,
            @JsonProperty("fullSql") String fullSql,
            @JsonProperty("dataCatalog") String dataCatalog,
            @JsonProperty("dataSchema") String dataSchema,
            @JsonProperty("metaCatalog") String metaCatalog,
            @JsonProperty("currentCatalog") String currentCatalog,
            @JsonProperty("currentSchema") String currentSchema,
            @JsonProperty("columns") List<MaterializedViewColumnHandle> columns,
            @JsonProperty("owner") String owner,
            @JsonProperty("runAsInvoker") boolean runAsInvoker,
            @JsonProperty("lastRefreshTime") String lastRefreshTime,
            @JsonProperty("status") String status)
    {
        this.viewName = requireNonNull(viewName, "view name is null");
        this.viewSql = requireNonNull(viewSql, "view sql is null");
        this.fullSql = fullSql;
        this.dataCatalog = dataCatalog;
        this.dataSchema = dataSchema;
        this.metaCatalog = metaCatalog;
        this.currentCatalog = currentCatalog;
        this.currentSchema = currentSchema;
        this.columns = columns;
        this.owner = owner;
        this.runAsInvoker = runAsInvoker;
        this.lastRefreshTime = MaterializedViewDateUtils.getDateFromString(lastRefreshTime);
        this.status = MaterializedViewStatus.getStatusFromString(status);
    }

    @JsonProperty
    public String getViewName()
    {
        return this.viewName;
    }

    @JsonProperty
    public String getViewSql()
    {
        return this.viewSql;
    }

    @JsonProperty
    public String getFullSql()
    {
        return this.fullSql;
    }

    @JsonProperty
    public String getDataCatalog()
    {
        return this.dataCatalog;
    }

    @JsonProperty
    public String getDataSchema()
    {
        return this.dataSchema;
    }

    @JsonProperty
    public String getMetaCatalog()
    {
        return this.metaCatalog;
    }

    @JsonProperty
    public String getCurrentCatalog()
    {
        return this.currentCatalog;
    }

    @JsonProperty
    public String getCurrentSchema()
    {
        return this.currentSchema;
    }

    @JsonProperty
    public List<MaterializedViewColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public String getOwner()
    {
        return owner;
    }

    @JsonProperty
    public boolean isRunAsInvoker()
    {
        return runAsInvoker;
    }

    @JsonProperty
    public String getLastRefreshTime()
    {
        return MaterializedViewDateUtils.getStringFromDate(lastRefreshTime);
    }

    @JsonProperty
    public String getStatus()
    {
        return MaterializedViewStatus.getStatusString(status);
    }

    public void setStatus(MaterializedViewStatus status)
    {
        this.status = status;
    }

    public void setLastRefreshTime()
    {
        this.lastRefreshTime = new Date();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("viewName", viewName)
                .add("viewSql", viewSql)
                .toString();
    }

    /**
     * parse Material view to json
     *
     * @return JSONObject
     * @throws JSONException JSONException
     */
    public JSONObject parseMaterialViewToJson()
            throws JSONException
    {
        JSONObject jo = new JSONObject();
        jo.put("viewName", viewName);
        jo.put("viewSql", viewSql);
        jo.put("fullSql", fullSql);
        jo.put("dataCatalog", dataCatalog);
        jo.put("dataSchema", dataSchema);
        jo.put("metaCatalog", metaCatalog);
        jo.put("currentCatalog", currentCatalog);
        jo.put("currentSchema", currentSchema);

        JSONArray jac = new JSONArray();
        for (MaterializedViewColumnHandle handle : columns) {
            jac.put(handle.toJsonObject());
        }
        jo.accumulate("columns", jac);
        jo.put("owner", owner);
        jo.put("runAsInvoker", runAsInvoker);
        jo.put("lastRefreshTime", MaterializedViewDateUtils.getStringFromDate(lastRefreshTime));
        jo.put("status", MaterializedViewStatus.getStatusString(status));

        return jo;
    }
}
