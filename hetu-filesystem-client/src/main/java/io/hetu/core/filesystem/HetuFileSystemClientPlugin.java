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
package io.hetu.core.filesystem;

import com.google.common.collect.ImmutableList;
import io.hetu.core.filesystem.db.DbFileSystemClientFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.filesystem.HetuFileSystemClientFactory;

/**
 * Hetu plugin for HetuFileSystemClient
 *
 * @since 2020-03-30
 */
public class HetuFileSystemClientPlugin
        implements Plugin
{
    @Override
    public Iterable<HetuFileSystemClientFactory> getFileSystemClientFactory()
    {
        return ImmutableList.of(new LocalFileSystemClientFactory(), new HdfsFileSystemClientFactory(), new DbFileSystemClientFactory());
    }
}
