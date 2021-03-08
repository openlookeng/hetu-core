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
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.hetu.core.metastore.hetufilesystem.HetuFsMetastore;
import io.hetu.core.metastore.hetufilesystem.HetuFsMetastoreConfig;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.metastore.HetuMetastore;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHeuristicIndexClient
{
    @Test
    public void testDeleteSelectedColumnsHelper()
            throws IOException
    {
        String tableName = "catalog.schema.UT_test";

        try (TempFolder folder = new TempFolder()) {
            // root/catalog.schema.UT_test/testColumn/bloom/testIndex.index
            folder.create();
            File tableFolder = new File(folder.getRoot().getPath(), tableName);
            assertTrue(tableFolder.mkdir());
            File columnFolder = new File(tableFolder, "testColumn");
            assertTrue(columnFolder.mkdirs());
            File indexTypeFolder = new File(columnFolder, "BLOOM");
            assertTrue(indexTypeFolder.mkdirs());
            assertTrue(new File(indexTypeFolder, "testIndex.index").createNewFile());

            HetuFileSystemClient fs = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), folder.getRoot().toPath());
            HetuMetastore testMetaStore = new HetuFsMetastore(new HetuFsMetastoreConfig().setHetuFileSystemMetastorePath(folder.getRoot().getPath()), fs);

            HeuristicIndexClient client = new HeuristicIndexClient(fs, testMetaStore, folder.getRoot().toPath());
            client.addIndexRecord(new CreateIndexMetadata("idx1",
                    tableName,
                    "BLOOM",
                    Collections.singletonList(new Pair<>("testColumn", VARCHAR)),
                    Collections.emptyList(),
                    new Properties(),
                    "user",
                    CreateIndexMetadata.Level.UNDEFINED));
            client.deleteIndex("idx1", Collections.emptyList());

            assertFalse(indexTypeFolder.exists());
        }
    }
}
