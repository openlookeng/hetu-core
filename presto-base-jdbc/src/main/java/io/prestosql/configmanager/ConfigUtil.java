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
 * Config Utils
 *
 * @since 2019-12-31
 */
public class ConfigUtil
{
    private ConfigUtil()
    {
    }

    /**
     * Build Config Search Pattern
     *
     * @param propertyNames property module names
     * @return Search Pattern
     */
    public static String buildConfigSearchPattern(String[] propertyNames)
    {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < propertyNames.length - 1; i++) {
            String name = propertyNames[i];
            result.append(name).append(ConfigConstants.PATTERN_SPLIT);
        }
        result.append(propertyNames[propertyNames.length - 1]);
        return result.toString();
    }

    /**
     * version file name builder, build File Name From Connector Name And Version Name
     * Every config version of a connector has a config file named 'connector-configurations-version.yaml'
     *
     * @param connectorName connectorName
     * @param versionName versionName
     * @return file name connector-version-configurations.yaml
     */
    public static String buildFileNameFromCoNameAndVerName(String connectorName, String versionName)
    {
        String mod = ConfigConstants.CONNECTOR_INJECT_CONFIG_FILE_NAME;
        mod = mod.replace("#1", connectorName);
        mod = mod.replace("#2", versionName);
        return mod;
    }
}
