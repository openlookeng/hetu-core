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
package io.prestosql.configmanager.fileloader;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Config File Parser Group
 * use a file name as the specific identify of a file
 *
 * @since 2019-12-18
 */
public final class ConfigFileParserGroup
{
    private static Map<String, ConfigFileParser> factoryInstances = new ConcurrentHashMap<>(Collections.emptyMap());

    /**
     * the constructor
     */
    private ConfigFileParserGroup()
    {
    }

    /**
     * refresh the ConfigFileParser instance
     */
    public static void refreshFactoryInstances()
    {
        synchronized (ConfigFileParserGroup.class) {
            for (Map.Entry<String, ConfigFileParser> entry : factoryInstances.entrySet()) {
                ConfigFileParser configFileParser = entry.getValue();
                if (!configFileParser.getIsConfigFileInJar() && configFileParser.isConfigFileModified()) {
                    configFileParser.reloadConfigRewriteMap();
                }
            }
        }
    }

    /**
     * remove config file parser by specific file path
     *
     * @param configFileName config file name
     */
    public static void removeConfigFileParser(String configFileName)
    {
        requireNonNull(configFileName, " file name requires no null...");
        synchronized (ConfigFileParserGroup.class) {
            factoryInstances.remove(configFileName);
        }
    }

    /**
     * is contain file
     *
     * @param configFileName config file name
     * @return if config file is loaded
     */
    public static boolean isContainFile(String configFileName)
    {
        requireNonNull(configFileName, " file name requires no null...");
        synchronized (ConfigFileParserGroup.class) {
            return factoryInstances.containsKey(configFileName);
        }
    }

    /**
     * the instance loader for singleton factory it will create a new instance using the config File path if there does
     * not exist one
     *
     * @param fileName fileName
     * @return the single instance for a special class
     */
    public static Optional<ConfigFileParser> getConfigFileParserInstance(String fileName)
    {
        requireNonNull(fileName, " file name requires no null in getInstance");
        ConfigFileParser instance = factoryInstances.get(fileName);
        if (instance == null) {
            synchronized (ConfigFileParserGroup.class) {
                if (!factoryInstances.containsKey(fileName)) {
                    return Optional.empty();
                }
                else {
                    instance = factoryInstances.get(fileName);
                }
            }
        }
        return Optional.of(instance);
    }

    /**
     * the instance loader for singleton factory it will create a new instance using the config File path if there does
     * not exist one
     *
     * @param fileName file name
     * @param configFilePath config file path
     */
    public static void addConfigFileParserInstance(String fileName, String configFilePath)
    {
        requireNonNull(fileName, " file name requires no null in getInstance");
        requireNonNull(configFilePath, " file path requires no null in getInstance");
        if (!factoryInstances.containsKey(fileName)) {
            synchronized (ConfigFileParserGroup.class) {
                if (!factoryInstances.containsKey(fileName)) {
                    ConfigFileParser instance = new ConfigFileParser(fileName, configFilePath);
                    factoryInstances.put(fileName, instance);
                }
            }
        }
    }
}
