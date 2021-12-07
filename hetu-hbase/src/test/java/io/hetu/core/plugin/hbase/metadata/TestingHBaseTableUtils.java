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
package io.hetu.core.plugin.hbase.metadata;

import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;

/**
 * TestJsonHBaseTableUtils
 *
 * @since 2020-03-20
 */
public class TestingHBaseTableUtils
{
    private static final Logger log = Logger.get(TestingHBaseTableUtils.class);

    private TestingHBaseTableUtils() {}

    /**
     * delFile
     */
    public static void delFile(String file)
    {
        File pfile = new File(file);
        if (!pfile.delete()) {
            log.info("File %s deletion failed", file);
        }
    }

    /**
     * createFile
     */
    public static void createFile(String filename)
            throws Exception
    {
        File file = new File(filename);
        if (!file.exists()) {
            try {
                file.createNewFile();
            }
            catch (IOException e) {
                throw new IOException("createFile " + filename + " : failed");
            }
        }
    }
}
