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
package io.hetu.core.filesystem.db;

import io.airlift.log.Logger;

import java.util.Properties;

/**
 * 动态目录数据库配置
 */
public class DbConfig {

    private static final Logger LOG = Logger.get(DbConfig.class);

    private static final String FS_DB_URL = "fs.db.url";

    private static final String FS_DB_DRIVER = "fs.db.driver";

    private static final String FS_DB_USERNAME = "fs.db.username";

    private static final String FS_DB_PASSWORD = "fs.db.password";

    private static final String FS_DB_INITIAL_SIZE = "fs.db.initialSize";

    private static final String FS_DB_MIN_IDLE = "fs.db.minIdle";

    private static final String FS_DB_MAX_ACTIVE = "fs.db.maxActive";

    private static final String FS_DB_MAX_WAIT = "fs.db.maxWait";

    private Properties dbProperties;

    public Properties getDbProperties() {
        return dbProperties;
    }

    public DbConfig(Properties properties) {
        generateDbConfig(properties);
    }

    private void generateDbConfig(Properties properties) {
        try {
            dbProperties = new Properties();
            dbProperties.setProperty("url", properties.getProperty(FS_DB_URL));
            dbProperties.setProperty("driverClassName", properties.getProperty(FS_DB_DRIVER));
            dbProperties.setProperty("username", properties.getProperty(FS_DB_USERNAME));
            dbProperties.setProperty("password", properties.getProperty(FS_DB_PASSWORD));
            dbProperties.setProperty("initialSize", configDefault(properties, FS_DB_INITIAL_SIZE, "1"));
            dbProperties.setProperty("minIdle", configDefault(properties, FS_DB_MIN_IDLE, "1"));
            dbProperties.setProperty("maxActive", configDefault(properties, FS_DB_MAX_ACTIVE, "1"));
            dbProperties.setProperty("maxWait", configDefault(properties, FS_DB_MAX_WAIT, "1"));
        } catch (Exception e) {
            LOG.error("获取动态目录数据库配置异常, 原因: {}", e.getMessage(), e);
            throw new IllegalArgumentException("获取动态目录数据库配置异常, 原因: " + e.getMessage());
        }
    }

    private String configDefault(Properties properties, String key, String defaultValue) {
        Object o = properties.get(key);
        return o != null && o != "" ? o.toString() : defaultValue;
    }
}
