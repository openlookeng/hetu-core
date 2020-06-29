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

import io.airlift.log.Logger;
import io.prestosql.configmanager.fileloader.ConfigFileParser;
import io.prestosql.configmanager.fileloader.ConfigFileParserGroup;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Config Dynamic Loader
 * It use the ConfigFileParserGroup to manager the group of specific filenames
 *
 * @since 2019-12-26
 */
public class ConfigManager
{
    private static volatile ConfigManager instance;

    private final Logger logger = Logger.get(ConfigManager.class);

    private ConfigVersionFileHandler configVersionFileHandler;

    private String connectorName;

    /**
     * the constructor
     */
    private ConfigManager(ConfigVersionFileHandler configVersionFileHandler, String connectorName)
    {
        requireNonNull(configVersionFileHandler);
        requireNonNull(connectorName);
        this.connectorName = connectorName;
        this.configVersionFileHandler = configVersionFileHandler;
        Map<String, String> configFileMap = configVersionFileHandler.getFileMap();
        addConfigFiles(configFileMap);
        threadStart();
    }

    private void threadStart()
    {
        RefreshThread refreshThread = new RefreshThread();
        refreshThread.setDaemon(true);
        refreshThread.setName(connectorName + "'s ConnectorConfigFunctionRewriteThread");
        refreshThread.start();
    }

    /**
     * add a config file to dynamic loader
     * filename:filepath
     *
     * @param fileMap the config file paths's map
     */
    private void addConfigFiles(Map<String, String> fileMap)
    {
        requireNonNull(fileMap, " file path requires no null in addConfigFile");
        for (Map.Entry<String, String> entry : fileMap.entrySet()) {
            addConfigFile(entry.getKey(), entry.getValue());
        }
    }

    /**
     * add a config file to dynamic loader
     *
     * @param fileName fileName
     * @param filePath the config file path
     */
    public void addConfigFile(String fileName, String filePath)
    {
        requireNonNull(fileName);
        requireNonNull(filePath);
        ConfigFileParserGroup.addConfigFileParserInstance(fileName, filePath);
    }

    /**
     * get a config items from a specific config [connector name]-[version]-[module:...:property's name]
     *
     * @param connectorName connectorName
     * @param versionName config version name
     * @param configModuleNames Config Module Names include property item name
     * @return Optional of the property's String values, it can be empty
     */
    public Optional<String> getConfigPropertyValue(String connectorName, String versionName, String... configModuleNames)
    {
        requireNonNull(connectorName);
        requireNonNull(versionName);
        requireNonNull(configModuleNames);
        String configSearchPattern = ConfigUtil.buildConfigSearchPattern(configModuleNames);
        String fileName = ConfigUtil.buildFileNameFromCoNameAndVerName(connectorName, versionName);
        Optional<Object> optionalO = searchConfigValues(fileName, configSearchPattern);
        if (optionalO.isPresent()) {
            try {
                return Optional.of((String) optionalO.get());
            }
            catch (ClassCastException cce) {
                logger.error("error config cast to string... ");
                return Optional.empty();
            }
        }
        else {
            return Optional.empty();
        }
    }

    private Optional<Object> searchConfigValues(String configFileName, String pattern)
    {
        Optional<ConfigFileParser> configFileParserOp = ConfigFileParserGroup.getConfigFileParserInstance(configFileName);
        if (configFileParserOp.isPresent()) {
            ConfigFileParser configFileParser = configFileParserOp.get();
            Map<String, Object> yamlMap = configFileParser.loadConfigModuleMap();
            String[] patternNames = pattern.split(ConfigConstants.PATTERN_SPLIT);
            for (int i = 0; i < patternNames.length - 1; i++) {
                if (yamlMap.containsKey(patternNames[i])) {
                    try {
                        yamlMap = (Map<String, Object>) yamlMap.get(patternNames[i]);
                    }
                    catch (ClassCastException cce) {
                        logger.error("error config cast to map... ");
                        return Optional.empty();
                    }
                }
                else {
                    logger.info("no such items in pattern... " + pattern);
                    return Optional.empty();
                }
            }
            try {
                Object values = yamlMap.get(patternNames[patternNames.length - 1]);
                if (values != null) {
                    return Optional.of(values);
                }
                else {
                    logger.info("no such items in pattern... " + pattern);
                    return Optional.empty();
                }
            }
            catch (ClassCastException cce) {
                logger.error("error config cast to string... ");
                return Optional.empty();
            }
        }
        else {
            logger.info("no such file named: " + configFileName);
            return Optional.empty();
        }
    }

    /**
     * get config map from a specific version and config module name
     * once you get the config map of a specific version and config module, you are responsible for ensuring data consistency
     *
     * @param connectorName connector name
     * @param versionName config version name
     * @param configModuleNames Config Module Names do not include property item name
     * @return config map, it can be empty
     */
    @Deprecated
    public Map<String, String> getConfigItemsMap(String connectorName, String versionName, String... configModuleNames)
    {
        requireNonNull(versionName);
        requireNonNull(configModuleNames);
        String configSearchPattern = ConfigUtil.buildConfigSearchPattern(configModuleNames);
        String fileName = ConfigUtil.buildFileNameFromCoNameAndVerName(connectorName, versionName);
        Optional<Object> optionalO = searchConfigValues(fileName, configSearchPattern);
        if (optionalO.isPresent()) {
            try {
                return (Map<String, String>) optionalO.get();
            }
            catch (ClassCastException cce) {
                logger.error("error config cast to string... ");
                return Collections.emptyMap();
            }
        }
        else {
            return Collections.emptyMap();
        }
    }

    /**
     * remove a config file from the dynamic loader
     *
     * @param configFileName file name
     */
    public void removeConfigFile(String configFileName)
    {
        requireNonNull(configFileName, " file path requires no null in removeConfigFile");
        ConfigFileParserGroup.removeConfigFileParser(configFileName);
    }

    private class RefreshThread
            extends Thread
    {
        private static final long SLEEP_MILLION_SECOND = 10000;

        /**
         * When an object implementing interface <code>Runnable</code> is used to create a thread, starting the thread
         * causes the object's <code>run</code> method to be called in that separately executing thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public void run()
        {
            logger.info("Dynamic loader %s start at %d", Thread.currentThread().getName(), System.currentTimeMillis());
            while (true) {
                ConfigFileParserGroup.refreshFactoryInstances();
                try {
                    Thread.sleep(SLEEP_MILLION_SECOND);
                }
                catch (InterruptedException e) {
                    logger.warn(e.getMessage());
                }
            }
        }
    }

    /**
     * ConfigManager is design as singleton, every connector/application can only construct one ConfigManager instance
     *
     * @param configVersionFileHandler config Version File Handler
     * @param connectorName  connector/application's Name
     * @return an instance of ConfigManager
     */
    public static ConfigManager newInstance(ConfigVersionFileHandler configVersionFileHandler, String connectorName)
    {
        requireNonNull(configVersionFileHandler);
        requireNonNull(connectorName);
        if (instance == null) {
            synchronized (ConfigManager.class) {
                if (instance == null) {
                    instance = new ConfigManager(configVersionFileHandler, connectorName);
                }
            }
        }
        return instance;
    }
}
