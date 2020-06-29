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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Configuration Version Controller
 * It contain the [file's name]:[file's path]
 * You must use it to construct a ConfigManager instance
 *
 * @since 2019-12-26
 */
public final class ConfigVersionFileHandler
{
    private Map<String, String> moduleVersionFiles;

    public ConfigVersionFileHandler()
    {
        this.moduleVersionFiles = new ConcurrentHashMap<>(Collections.emptyMap());
    }

    public Optional<String> getFilePathFromVersion(String versionFileName)
    {
        requireNonNull(versionFileName);
        return Optional.ofNullable(this.moduleVersionFiles.get(versionFileName));
    }

    public Map<String, String> getFileMap()
    {
        return new HashMap<>(moduleVersionFiles);
    }

    public void addConfigVersionFiles(Map<String, String> versionFiles)
    {
        requireNonNull(versionFiles, " moduleFiles require not null in ConfigVersionController");
        this.moduleVersionFiles.putAll(versionFiles);
    }

    /**
     * this method will refresh the config file for a specific version
     *
     * @param versionFileName versionName
     * @param filePath filePath
     */
    public void addVersionConfigFile(String versionFileName, String filePath)
    {
        moduleVersionFiles.put(versionFileName, filePath);
    }
}
