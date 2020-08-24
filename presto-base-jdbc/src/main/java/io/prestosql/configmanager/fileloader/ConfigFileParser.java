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

import io.airlift.log.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Config File Parser
 *
 * @since 2019-12-26
 */
public final class ConfigFileParser
{
    private final Logger logger = Logger.get(ConfigFileParser.class);

    private String configFilePath;

    private String fileName;

    private String absFilePathWithFileName;

    private Yaml yaml;

    private Map<String, Object> yamlMaps;

    private boolean isYamlFileInJar;

    private long configFileLastModified = -1;

    /**
     * the constructor
     *
     * @param fileName fileName
     * @param yamlFilePath you can config the different file path
     */
    public ConfigFileParser(String fileName, String yamlFilePath)
    {
        this.configFilePath = requireNonNull(yamlFilePath);
        this.fileName = requireNonNull(fileName);
        Path path = Paths.get(configFilePath, fileName);
        this.absFilePathWithFileName = path.toString();
        reloadConfigRewriteMap();
    }

    public boolean getIsConfigFileInJar()
    {
        return isYamlFileInJar;
    }

    public void reloadConfigRewriteMap()
    {
        InputStream inputStream;
        // to find it in jar file properties
        // UT and runtime pass way
        inputStream = this.getClass().getClassLoader().getResourceAsStream(fileName);
        isYamlFileInJar = true;
        if (inputStream == null) {
            logger.debug("yaml config path error...");
        }
        else {
            // parser the yaml file and save to matcherMap
            this.yaml = new Yaml();
            try {
                this.yamlMaps = new ConcurrentHashMap<>(yaml.load(inputStream));
            }
            catch (ClassCastException cls) {
                logger.info("Error format in the yaml config file!");
                this.yamlMaps = new ConcurrentHashMap<>(Collections.emptyMap());
            }
            try {
                inputStream.close();
            }
            catch (IOException e) {
                logger.debug("error closing stream in config load.");
            }
        }
    }

    /**
     * get a config module map by module name from yaml file
     *
     * @return the config key value map, it can be empty
     */
    public Map<String, Object> loadConfigModuleMap()
    {
        if (yamlMaps != null) {
            return new ConcurrentHashMap<>(this.yamlMaps);
        }
        else {
            return Collections.emptyMap();
        }
    }

    /**
     * to get the result if the yaml file is modify or not
     *
     * @return true for having been modified and false for not
     */
    public boolean isConfigFileModified()
    {
        if (isYamlFileInJar || absFilePathWithFileName == null) {
            return false;
        }
        File file;
        file = new File(absFilePathWithFileName);
        if (!file.exists()) {
            logger.info("Config file: %s path does not exists", absFilePathWithFileName);
            return false;
        }
        boolean isChange = false;
        if (file.lastModified() > configFileLastModified) {
            logger.info("config file change in %s ", absFilePathWithFileName);
            configFileLastModified = file.lastModified();
            isChange = true;
        }
        return isChange;
    }
}
