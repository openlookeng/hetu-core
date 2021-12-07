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
package io.hetu.core.plugin.heuristicindex.index.btree;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

import static org.testng.Assert.assertEquals;

public class TestSnappyCompressionSerializer
{
    static Random random = new Random(System.nanoTime());

    @Test
    public void testSnappy() throws IOException
    {
        String dir = "/tmp/" + String.valueOf(300 + random.nextInt(100));
        String file = dir + "/index.bt";
        try {
            if (Files.exists(Paths.get(file))) {
                Files.delete(Paths.get(file));
            }
            Files.createDirectories(Paths.get(dir));
            DB db = DBMaker
                    .fileDB(file)
                    .fileMmapEnableIfSupported()
                    .make();
            BTreeMap btree = db.treeMap("map")
                    .keySerializer(Serializer.STRING)
                    .valueSerializer(new SnappyCompressionSerializer<>(Serializer.STRING))
                    .create();
            int entries = 200;
            String value = "001:3,002:3,003:3,004:3,005:3,006:3,007:3,008:3,009:3,002:3,010:3,002:3,011:3,012:3,101:3,102:3,103:3,104:3,105:3,106:3,107:3,108:3,109:3,102:3,110:3,102:3,111:3,112:3";
            for (int i = 0; i < entries; i++) {
                btree.put(i + "", value);
            }
            assertEquals(value, btree.get("99"));
        }
        finally {
            Files.deleteIfExists(Paths.get(file));
            Files.delete(Paths.get(dir));
        }
    }
}
