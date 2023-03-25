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

import io.hetu.core.filesystem.LocalFileSystemClientFactory;
import io.prestosql.spi.filesystem.HetuFileSystemClient;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * 动态目录数据库客户端工厂
 */
public class DbFileSystemClientFactory extends LocalFileSystemClientFactory {
    private static final String NAME_DB = "db";

    @Override
    public HetuFileSystemClient getFileSystemClient(Properties properties) {
        return new HetuDbFileSystemClient(new DbConfig(properties), Paths.get("/"));
    }

    @Override
    public HetuFileSystemClient getFileSystemClient(Properties properties, Path root) {
        return new HetuDbFileSystemClient(new DbConfig(properties), root);
    }

    @Override
    public String getName() {
        return NAME_DB;
    }
}
