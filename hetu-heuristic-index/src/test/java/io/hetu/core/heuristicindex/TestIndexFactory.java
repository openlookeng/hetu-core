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
import io.hetu.core.heuristicindex.base.BloomIndex;
import io.hetu.core.heuristicindex.base.LocalIndexStore;
import io.hetu.core.heuristicindex.base.MinMaxIndex;
import io.hetu.core.spi.heuristicindex.DataSource;
import io.hetu.core.spi.heuristicindex.Index;
import io.hetu.core.spi.heuristicindex.IndexStore;
import io.hetu.core.spi.heuristicindex.SplitIndexMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestIndexFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(TestIndexFactory.class);

    private String indexStoreTypeKey = "type";
    private String localIndexStoreId = "LOCAL";
    private String indexStoreKeysPrefix = "hetu.filter.indexstore.";
    private String datastoreTypeKey = "connector.name";
    private String emptyDataSourceId = "empty";

    @Test
    public void testGetInstance()
    {
        assertTrue(IndexFactory.getInstance() == (IndexFactory.getInstance()),
                "getInstance should return same instance");
    }

    @Test
    public void testLoadPlugins() throws IOException
    {
        // no plugins
        IndexFactory.getInstance().loadPlugins(new String[]{});

        // plugins
    }

    @Test
    public void testCsvDataSourceLocalIndexStore() throws IOException
    {
        Properties dataSourceProps = new Properties();
        File csvResources = new File("src/test/resources/csv").getCanonicalFile();
        dataSourceProps.setProperty("connector.name", "csv");
        dataSourceProps.setProperty("rootDir", csvResources.getAbsolutePath());
        DataSource dataSource = new CsvDataSource();
        dataSource.setProperties(dataSourceProps);

        Properties indexStoreProps = new Properties();
        indexStoreProps.setProperty("type", "local");
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File indexstoreDir = folder.newFolder();
            indexstoreDir.deleteOnExit();
            indexStoreProps.setProperty("uri", indexstoreDir.getCanonicalPath());
            IndexStore indexStore = new LocalIndexStore();
            indexStore.setProperties(indexStoreProps);

            Set<Index> indexes = new HashSet<>();
            indexes.add(new MinMaxIndex());
            indexes.add(new BloomIndex());

            IndexWriter writer = new IndexWriter(dataSource, indexes, indexStore);

            String table = "csv.schemaName.tableName";
            String[] columns = new String[] {"0", "2"};
            String[] partitons = new String[] {"p=bar"};
            writer.createIndex(table, columns, partitons, "bloom", "minmax");

            IndexClient client = IndexFactory.getInstance().getIndexClient(indexStoreProps);
            List<SplitIndexMetadata> splits = client.readSplitIndex(table);
            // there should be 8 splits (2 csv files * 2 columns * 2 index types)
            assertEquals(8, splits.size());

            // read the index file for csv/schemaName/tableName/p=bar/000.csv, column 2
            File csvFile = new File("src/test/resources/csv/schemaName/tableName/p=bar/000.csv").getCanonicalFile();
            String filePath = Paths.get("csv.schemaName.tableName", "2", csvFile.toString()).toString();
            splits = client.readSplitIndex(filePath);
            // there should be 2 splits (1 csv file * 1 columns * 2 index types)
            assertEquals(2, splits.size());
            BloomIndex bloomIndex = null;
            MinMaxIndex minMaxIndex = null;
            for (SplitIndexMetadata split : splits) {
                LOG.info("read split: %s", split.toString());
                if (split.getIndex() instanceof BloomIndex) {
                    bloomIndex = (BloomIndex) split.getIndex();
                }
                else if (split.getIndex() instanceof MinMaxIndex) {
                    minMaxIndex = (MinMaxIndex) split.getIndex();
                }
            }

            assertNotNull(bloomIndex);
            assertNotNull(minMaxIndex);

            assertTrue(bloomIndex.mightContain(22));
            assertFalse(bloomIndex.mightContain(99));

            assertTrue(minMaxIndex.lessThan(25));
            assertFalse(minMaxIndex.lessThan(15));
        }
    }

    @Test
    public void testLoadIndexStoreProperties() throws IOException
    {
        // null index store type
        assertThrows(NullPointerException.class, () -> IndexFactory.getInstance().getIndexClient((Properties) null));
        assertThrows(NullPointerException.class, () -> IndexFactory.getInstance().getIndexClient((String) null));

        // Empty index store type
        Properties props = new Properties();
        props.setProperty(indexStoreTypeKey, "");
        assertThrows(RuntimeException.class, () -> IndexFactory.getInstance().getIndexClient(props));

        // Loading existing indexstore
        props.clear();
        props.setProperty(indexStoreTypeKey, localIndexStoreId);
        assertNotNull(IndexFactory.getInstance().getIndexClient(props));

        // Loading exsiting indexstore from a file
        props.clear();
        props.setProperty(indexStoreKeysPrefix + indexStoreTypeKey, localIndexStoreId);
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File tf = folder.newFile("config.properties");
            try (OutputStream os = new FileOutputStream(tf)) {
                props.store(os, "Indexstore UT");
            }
            assertNotNull(IndexFactory.getInstance().getIndexClient(folder.getRoot().getCanonicalPath()));
        }
    }

    @Test
    public void testLoadDataSourceProperties() throws IOException
    {
        // Null check
        assertThrows(NullPointerException.class, () -> IndexFactory.getInstance().getIndexWriter((Properties) null, (Properties) null, (Properties) null));
        assertThrows(NullPointerException.class, () -> IndexFactory.getInstance().getIndexWriter((String) null, (String) null));

        // Empty data source type
        Properties dsProps = new Properties();
        Properties isProps = new Properties();
        Properties ixProps = new Properties();
        dsProps.setProperty(datastoreTypeKey, "");
        assertThrows(RuntimeException.class, () -> IndexFactory.getInstance().getIndexWriter(dsProps, isProps, ixProps));

        // loading empty data source, but empty indexstore
        isProps.setProperty(indexStoreTypeKey, "");
        dsProps.setProperty(datastoreTypeKey, emptyDataSourceId);
        assertThrows(RuntimeException.class, () -> IndexFactory.getInstance().getIndexWriter(dsProps, isProps, ixProps));

        // loading empty data source, local indexstore
        isProps.setProperty(indexStoreTypeKey, localIndexStoreId);
        dsProps.setProperty(datastoreTypeKey, emptyDataSourceId);
        assertNotNull(IndexFactory.getInstance().getIndexWriter(dsProps, isProps, ixProps));
    }

    @Test
    public void testLoadIndexWriterFromConfigFile() throws IOException
    {
        Properties dsProps = new Properties();
        Properties isProps = new Properties();

        isProps.setProperty(indexStoreKeysPrefix + indexStoreTypeKey, localIndexStoreId);
        dsProps.setProperty(datastoreTypeKey, emptyDataSourceId);

        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File conf = folder.newFile("config.properties");
            try (OutputStream os = new FileOutputStream(conf)) {
                isProps.store(os, "Index Writer UT");
            }
            File catalogFolder = folder.newFolder("catalog");
            File catalogConf = new File(catalogFolder, "test.properties");
            try (OutputStream os = new FileOutputStream(catalogConf)) {
                dsProps.store(os, "Index Writer UT");
            }
            assertNotNull(IndexFactory.getInstance().getIndexWriter("test.random.stuff",
                    folder.getRoot().getCanonicalPath()));
        }
    }
}
