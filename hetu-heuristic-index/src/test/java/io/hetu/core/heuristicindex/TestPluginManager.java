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
package io.hetu.core.heuristicindex;

import io.hetu.core.common.filesystem.TempFolder;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertNotNull;

public class TestPluginManager
{
    @Test
    public void testLoadPluginsSimple() throws IOException
    {
        PluginManager manager = new PluginManager();
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            folder.newFile("plugin1.jar");
            folder.newFile("plugin2.jar");

            String[] pluginsLocation = new String[] {folder.getRoot().getCanonicalPath()};
            manager.loadPlugins(pluginsLocation);
        }
    }

    @Test
    public void testLoadNativePlugins() throws IOException
    {
        PluginManager manager = new PluginManager();
        manager.loadNativePlugins();

        // existing datasource, indices, and indexstore should be loaded
        assertNotNull(manager.getDataSource("empty"));
        assertNotNull(manager.getIndex("minmax"));
        assertNotNull(manager.getIndex("bloom"));
        assertNotNull(manager.getIndexStore("local"));
    }
}
