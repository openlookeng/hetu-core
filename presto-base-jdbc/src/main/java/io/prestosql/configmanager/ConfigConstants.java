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
package io.prestosql.configmanager;

/**
 * Config Constants
 * @since 2019-12-28
 */
public class ConfigConstants
{
    private ConfigConstants()
    {
    }

    /**
     * config pattern split string
     */
    public static final String PATTERN_SPLIT = ":";

    /**
     * udf yaml file module name
     */
    public static final String CONFIG_UDF_MODULE_NAME = "rewrite_functions";

    /**
     * connector config file name mod
     */
    public static final String CONNECTOR_INJECT_CONFIG_FILE_NAME = "#1-#2-configurations.yml";

    /**
     * the default version name
     */
    public static final String DEFAULT_VERSION_NAME = "default";
}
