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

package io.hetu.core.materializedview.utils;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.hetu.core.materializedview.metadata.MaterializedView;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

/**
 * Mv json utils
 *
 * @since 2020-03-26
 */
public class MaterializedViewJsonUtils
{
    private static final Logger LOG = Logger.get(MaterializedViewJsonUtils.class);
    private static final JsonCodec<MaterializedViewMetadataSpec> CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(MaterializedViewMetadataSpec.class);

    private MaterializedViewJsonUtils()
    {
    }

    /**
     * convert json to material views
     *
     * @param materialViews mvs
     */
    public static void loadMaterialViewFromJson(Set<String> schemas, Map<String, Map<String, MaterializedView>> materialViews, byte[] jsons)
    {
        if (jsons == null || jsons.length == 0) {
            return;
        }
        MaterializedViewMetadataSpec metadataSpec = CODEC.fromJson(jsons);
        if (metadataSpec != null) {
            schemas.addAll(metadataSpec.getSchemas());
            for (Map.Entry<String, List<MaterializedView>> entry : metadataSpec.getViews().entrySet()) {
                String schema = entry.getKey();
                List<MaterializedView> views = entry.getValue();
                Map<String, MaterializedView> viewMap = new HashMap<>();
                for (MaterializedView mv : views) {
                    viewMap.put(mv.getViewName(), mv);
                }
                materialViews.put(schema, viewMap);
            }
        }
    }

    public static JSONObject materialViewsToJson(Map<String, Map<String, MaterializedView>> materialViews, Set<String> schemas)
    {
        JSONObject mvJson = new JSONObject();

        try {
            JSONArray schemaJson = new JSONArray();
            for (String schema : schemas) {
                schemaJson.put(schema);
            }

            mvJson.put("schemas", schemaJson);

            JSONObject viewsJson = new JSONObject();

            for (Map.Entry<String, Map<String, MaterializedView>> entry : materialViews.entrySet()) {
                JSONArray schemaViewJson = new JSONArray();
                String schemaName = entry.getKey();
                Map<String, MaterializedView> schemaViews = entry.getValue();
                for (Map.Entry<String, MaterializedView> viewEntry : schemaViews.entrySet()) {
                    schemaViewJson.put(viewEntry.getValue().parseMaterialViewToJson());
                }
                viewsJson.put(schemaName, schemaViewJson);
            }

            mvJson.put("views", viewsJson);
        }
        catch (JSONException e) {
            LOG.error("Material View to Json failed: [%s]", e.getMessage());
        }

        return mvJson;
    }
}
