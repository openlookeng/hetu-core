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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestIndexWriter
{
    private static final Logger LOG = LoggerFactory.getLogger(TestIndexWriter.class);

    @Test
    public void testIndexWriterSimple() throws IOException
    {
        // Simple workflow without calling readSplit's callback
        DataSource ds = mock(DataSource.class);
        IndexStore is = createTestIndexStore();

        Set<Index> indices = new HashSet<>();
        indices.add(new BloomIndex());

        IndexWriter writer = new IndexWriter(ds, indices, is);

        // Runtime exception will be thrown because the table was empty
        assertThrows(RuntimeException.class, () -> writer.createIndex("catalog.schema.table", new String[]{"test"}, new String[]{}, "bloom"));

        // Null check
        assertThrows(RuntimeException.class, () -> new IndexWriter(null, null, null));

        // Invalid table format
        assertThrows(RuntimeException.class,
                () -> writer.createIndex("invalid", new String[]{"test"}, new String[]{}, "bloom"));
    }

    @Test
    public void testUnsupportedIndexType() throws IOException
    {
        DataSource ds = new TestDataSource();
        IndexStore is = createTestIndexStore();

        Set<Index> indices = new HashSet<>();
        indices.add(new BloomIndex());

        IndexWriter writer = new IndexWriter(ds, indices, is);
        String tableName = "catalog.schema.table";

        assertThrows(RuntimeException.class,
                () -> writer.createIndex(tableName, new String[]{"test"}, new String[]{}, "Random"));
        assertIndexWriterCleanUp(is, tableName);
    }

    @Test
    public void testIndexWriterCallback() throws IOException
    {
        DataSource ds = new TestDataSource();
        IndexStore is = createTestIndexStore();

        Set<Index> indices = new HashSet<>();
        indices.add(new BloomIndex());

        IndexWriter writer = new IndexWriter(ds, indices, is);
        String tableName = "catalog.schema.table";

        writer.createIndex(tableName, new String[]{"test"}, new String[]{}, "bloom");

        // Assert index files are created
        File indexFolder = new File(is.getProperties().getProperty(IndexStore.ROOT_URI_KEY) + "/" + tableName);
        assertTrue(indexFolder.listFiles().length > 0);
    }

    /**
     * create a bloom index followed by a minmax index
     *
     * the lastModifiedTime of the datasource split will change, this means
     * only the latter minmax index should remain
     * @throws IOException
     */
    @Test
    public void testIndexWriterMultipleWritesExpired() throws IOException
    {
        DataSource ds = new DataSource()
        {
            @Override
            public String getId()
            {
                return "test";
            }

            @Override
            public void readSplits(String schema, String table, String[] columns, String[] partitions, DataSource.Callback callback)
            {
                Object[] values = new Object[]{"test", "dsfdfs", "random"};
                callback.call("UT_test_column", values, "UT_test", 100, System.currentTimeMillis());
            }
        };

        IndexStore is = createTestIndexStore();

        Set<Index> indices = new HashSet<>();
        indices.add(new BloomIndex());
        indices.add(new MinMaxIndex());

        IndexWriter writer = new IndexWriter(ds, indices, is);
        String tableName = "catalog.schema.table";

        writer.createIndex(tableName, new String[]{"test"}, new String[]{}, "bloom");

        File indexFolder = new File(is.getProperties().getProperty(IndexStore.ROOT_URI_KEY) + "/" + tableName);
        LOG.info("Previous files:");
        Set<Path> previousFiles = Files.walk(Paths.get(indexFolder.getAbsolutePath()))
                .filter(Files::isRegularFile).collect(Collectors.toSet());
        previousFiles.forEach(f -> LOG.info(f.toString()));

        writer.createIndex(tableName, new String[]{"test"}, new String[]{}, "minmax");
        LOG.info("New files:");
        Set<Path> newFiles = Files.walk(Paths.get(indexFolder.getAbsolutePath()))
                .filter(Files::isRegularFile).collect(Collectors.toSet());
        newFiles.forEach(f -> LOG.info(f.toString()));

        // should be the minmax index file and lastmodified file
        assertEquals(newFiles.size(), 2);

        // all files should be different
        for (Path previousFile : previousFiles) {
            assertFalse(newFiles.contains(previousFile), "should not have found " + previousFile);
        }
    }

    /**
     * create a bloom index followed by a minmax index
     *
     * the lastModifiedTime of the datasource split will remain the same
     * this means both bloom and minmax index should remain with
     * the same lastModifiedFile
     * @throws IOException
     */
    @Test
    public void testIndexWriterMultipleWrites() throws IOException
    {
        DataSource ds = new DataSource()
        {
            @Override
            public String getId()
            {
                return "test";
            }

            @Override
            public void readSplits(String schema, String table, String[] columns, String[] partitions, DataSource.Callback
                    callback)
            {
                Object[] values = new Object[]{"test", "dsfdfs", "random"};
                callback.call("UT_test_column", values, "UT_test", 100, 123);
            }
        };

        IndexStore is = createTestIndexStore();

        Set<Index> indices = new HashSet<>();
        indices.add(new BloomIndex());
        indices.add(new MinMaxIndex());

        IndexWriter writer = new IndexWriter(ds, indices, is);
        String tableName = "catalog.schema.table";

        writer.createIndex(tableName, new String[]{"test"}, new String[]{}, "bloom");

        File indexFolder = new File(is.getProperties().getProperty(IndexStore.ROOT_URI_KEY) + "/" + tableName);
        LOG.info("Previous files:");
        Set<Path> previousFiles = Files.walk(Paths.get(indexFolder.getAbsolutePath()))
                .filter(Files::isRegularFile).collect(Collectors.toSet());
        previousFiles.forEach(f -> LOG.info(f.toString()));

        writer.createIndex(tableName, new String[]{"test"}, new String[]{}, "minmax");
        LOG.info("New files:");
        Set<Path> newFiles = Files.walk(Paths.get(indexFolder.getAbsolutePath()))
                .filter(Files::isRegularFile).collect(Collectors.toSet());
        newFiles.forEach(f -> LOG.info(f.toString()));

        // should be the minmax index file and lastmodified file
        assertEquals(newFiles.size(), 3);

        // previous files should still be there
        for (Path previousFile : previousFiles) {
            assertTrue(newFiles.contains(previousFile), "did not find " + previousFile);
        }
    }

    /**
     * create a bloom index with debug on, this will also write
     * the data of each split into a file alongside the index file
     * @throws IOException
     */
    @Test
    public void testDebugMode() throws IOException
    {
        DataSource ds = new DataSource()
        {
            @Override
            public String getId()
            {
                return "test";
            }

            @Override
            public void readSplits(String schema, String table, String[] columns, String[] partitions, DataSource.Callback
                    callback)
            {
                Object[] values = new Object[]{"test", "dsfdfs", "random"};
                callback.call("UT_test_column", values, "UT_test", 100, System.currentTimeMillis());
            }
        };

        IndexStore is = createTestIndexStore();

        Set<Index> indices = new HashSet<>();
        indices.add(new BloomIndex());

        IndexWriter writer = new IndexWriter(ds, indices, is);
        String tableName = "catalog.schema.table";

        writer.createIndex(tableName, new String[]{"test"}, new String[]{}, new String[]{"bloom"}, true, true);

        File indexFolder = new File(is.getProperties().getProperty(IndexStore.ROOT_URI_KEY) + "/" + tableName);
        LOG.info("Previous files:");
        Set<Path> files = Files.walk(Paths.get(indexFolder.getAbsolutePath()))
                .filter(Files::isRegularFile).collect(Collectors.toSet());
        files.forEach(f -> LOG.info(f.toString()));

        assertEquals(files.size(), 3);
    }

    private void assertIndexWriterCleanUp(IndexStore is, String tableName) throws IOException
    {
        File indexFolder = new File(is.getProperties().getProperty(IndexStore.ROOT_URI_KEY) + "/" + tableName);
        // TODO: note empty directories may be left behind
        assertTrue(Files.walk(Paths.get(indexFolder.getAbsolutePath())).filter(Files::isRegularFile).count() == 0,
                "all part files and lock files has to be deleted upon error");
    }

    private IndexStore createTestIndexStore() throws IOException
    {
        Properties props = new Properties();
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            props.setProperty(IndexStore.ROOT_URI_KEY, folder.getRoot().getCanonicalPath());

            IndexStore is = new LocalIndexStore();
            is.setProperties(props);

            return is;
        }
    }

    private static class TestDataSource
            implements DataSource
    {
        @Override
        public String getId()
        {
            return "test";
        }

        @Override
        public void readSplits(String schema, String table, String[] columns, String[] partitions, Callback callback)
                throws IOException
        {
            Object[] values = new Object[]{"test", "dsfdfs", "random"};
            callback.call("UT_test_column", values, "UT_test", 100, System.currentTimeMillis());
        }
    }
}
