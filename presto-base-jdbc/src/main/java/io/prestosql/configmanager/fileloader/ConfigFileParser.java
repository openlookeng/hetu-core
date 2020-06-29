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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
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
        if (absFilePathWithFileName != null) {
            InputStream inputStream;
            try {
                // to find properties file in a specific directory
                // runtime pass way1
                inputStream = new FileInputStream(new File(absFilePathWithFileName));
                logger.info("find yaml file in ConfigFileParser for a abs path: %s", absFilePathWithFileName);
            }
            catch (IOException e1) {
                logger.warn(e1.getMessage());
                logger.warn("can not find file: %s in the abs path, find in jar file path..", absFilePathWithFileName);
                try {
                    // find file in the jar file path
                    // runtime pass way2
                    String jarAbsPath = getJarFileAbsPath();
                    if (jarAbsPath != null) {
                        Path pathNew = Paths.get(jarAbsPath, fileName);
                        absFilePathWithFileName = pathNew.toString();
                        inputStream = new FileInputStream(new File(pathNew.toString()));
                    }
                    else {
                        throw new IOException("jarAbsPath is unexpected valued null...");
                    }
                }
                catch (IOException e2) {
                    logger.warn(e2.getMessage());
                    logger.warn("can not find file: %s in the abs jar path, find in jar..", absFilePathWithFileName);
                    // to find it in jar file properties
                    // UT pass way
                    inputStream = this.getClass().getClassLoader().getResourceAsStream(fileName);
                    isYamlFileInJar = true;
                }
            }
            if (inputStream == null) {
                logger.error("yaml config path error...");
            }
            else {
                // parser the yaml file and save to matcherMap
                this.yaml = new Yaml();
                try {
                    this.yamlMaps = new ConcurrentHashMap<>(yaml.load(inputStream));
                }
                catch (ClassCastException cls) {
                    logger.error("error format in the yaml file!!!!");
                    this.yamlMaps = new ConcurrentHashMap<>(Collections.emptyMap());
                }
                try {
                    inputStream.close();
                }
                catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    private String getJarFileAbsPath()
    {
        URL url = this.getClass().getProtectionDomain().getCodeSource().getLocation();
        String filePath;
        try {
            filePath = URLDecoder.decode(url.getPath(), "utf-8");
            if (filePath.endsWith(".jar")) {
                filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
            }
            File file = new File(filePath);
            filePath = file.getCanonicalPath();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
        return filePath;
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
