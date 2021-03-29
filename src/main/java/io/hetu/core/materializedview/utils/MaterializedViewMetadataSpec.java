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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.hetu.core.materializedview.metadata.MaterializedView;

import java.util.List;
import java.util.Map;

public class MaterializedViewMetadataSpec
{
    private final List<String> schemas;
    private final Map<String, List<MaterializedView>> views;

    @JsonCreator
    public MaterializedViewMetadataSpec(
            @JsonProperty("schemas") List<String> schemas,
            @JsonProperty("views") Map<String, List<MaterializedView>> views)
    {
        this.schemas = schemas;
        this.views = views;
    }

    public List<String> getSchemas()
    {
        return schemas;
    }

    public Map<String, List<MaterializedView>> getViews()
    {
        return views;
    }
}
