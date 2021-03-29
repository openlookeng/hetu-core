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

package io.hetu.core.materializedview.conf;

import io.airlift.configuration.Config;

/**
 * mv.properties
 *
 * @since 2020-03-25
 */
public class MaterializedViewConfig
{
    private String metastoreType;
    private String metastorePath;

    public String getMetastoreType()
    {
        return metastoreType;
    }

    @Config("mv.metastore.type")
    public void setMetastoreType(String metastoreType)
    {
        this.metastoreType = metastoreType;
    }

    public String getMetastorePath()
    {
        return metastorePath;
    }

    @Config("mv.metastore.path")
    public void setMetastorePath(String metastorePath)
    {
        this.metastorePath = metastorePath;
    }
}
