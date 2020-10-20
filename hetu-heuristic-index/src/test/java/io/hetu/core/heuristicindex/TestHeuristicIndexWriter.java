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
import io.hetu.core.plugin.heuristicindex.index.bloom.BloomIndex;
import io.hetu.core.plugin.heuristicindex.index.minmax.MinMaxIndex;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.DataSource;
import io.prestosql.spi.heuristicindex.Index;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
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

public class TestHeuristicIndexWriter
{
    private static final Logger LOG = LoggerFactory.getLogger(TestHeuristicIndexWriter.class);

    @Test
    public void testIndexWriterSimple()
            throws IOException
    {
        // Simple workflow without calling readSplit's callback
        DataSource ds = mock(DataSource.class);
        HetuFileSystemClient fs = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get("/"));

        Set<Index> indices = new HashSet<>();
        indices.add(new BloomIndex());

        HeuristicIndexWriter writer = new HeuristicIndexWriter(ds, indices, fs, null);

        // Runtime exception will be thrown because the table was empty
        assertThrows(RuntimeException.class, () -> writer.createIndex("catalog.schema.table", new String[] {"test"}, new String[] {}, "bloom"));

        // Null check
        assertThrows(RuntimeException.class, () -> new HeuristicIndexWriter(null, null, null, null));

        // Invalid table format
        assertThrows(RuntimeException.class,
                () -> writer.createIndex("invalid", new String[] {"test"}, new String[] {}, "bloom"));
    }

    @Test
    public void testUnsupportedIndexType()
            throws IOException
    {
        DataSource ds = new TestDataSource();
        try (TempFolder folder = new TempFolder()) {
            folder.create();

            HetuFileSystemClient fs = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), folder.getRoot().toPath());

            Set<Index> indices = new HashSet<>();
            indices.add(new BloomIndex());

            HeuristicIndexWriter writer = new HeuristicIndexWriter(ds, indices, fs, folder.getRoot().toPath());
            String tableName = "catalog.schema.table";

            assertThrows(RuntimeException.class,
                    () -> writer.createIndex(tableName, new String[] {"test"}, new String[] {}, "Random"));
            assertIndexWriterCleanUp(folder.getRoot().toPath(), tableName);
        }
    }

    @Test
    public void testIndexWriterCallback()
            throws IOException
    {
        DataSource ds = new TestDataSource();
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            HetuFileSystemClient fs = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), folder.getRoot().toPath());

            Set<Index> indices = new HashSet<>();
            indices.add(new BloomIndex());

            HeuristicIndexWriter writer = new HeuristicIndexWriter(ds, indices, fs, folder.getRoot().toPath());
            String tableName = "catalog.schema.table";

            writer.createIndex(tableName, new String[] {"test"}, new String[] {}, "bloom");

            // Assert index files are created
            File indexFolder = new File(folder.getRoot().getAbsolutePath() + "/" + tableName);
            assertTrue(indexFolder.listFiles().length > 0);
        }
    }

    /**
     * create a bloom index followed by a minmax index
     * <p>
     * the lastModifiedTime of the datasource split will change, this means
     * only the latter minmax index should remain
     *
     * @throws IOException
     */
    @Test
    public void testIndexWriterMultipleWritesExpired()
            throws IOException
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
                Object[] values = new Object[] {"test", "dsfdfs", "random"};
                callback.call("UT_test_column", values, "UT_test", 100, System.currentTimeMillis(), 0);
            }
        };
        try (TempFolder folder = new TempFolder()) {
            folder.create();

            HetuFileSystemClient fs = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), folder.getRoot().toPath());

            Set<Index> indices = new HashSet<>();
            indices.add(new BloomIndex());
            indices.add(new MinMaxIndex());

            HeuristicIndexWriter writer = new HeuristicIndexWriter(ds, indices, fs, folder.getRoot().toPath());
            String tableName = "catalog.schema.table";

            writer.createIndex(tableName, new String[] {"test"}, new String[] {}, "bloom");

            File indexFolder = new File(folder.getRoot().getAbsolutePath() + "/" + tableName);
            LOG.info("Previous files:");
            Set<Path> previousFiles = Files.walk(Paths.get(indexFolder.getAbsolutePath()))
                    .filter(Files::isRegularFile).collect(Collectors.toSet());
            previousFiles.forEach(f -> LOG.info(f.toString()));

            writer.createIndex(tableName, new String[] {"test"}, new String[] {}, "bloom");
            LOG.info("New files:");
            Set<Path> newFiles = Files.walk(Paths.get(indexFolder.getAbsolutePath()))
                    .filter(Files::isRegularFile).collect(Collectors.toSet());
            newFiles.forEach(f -> LOG.info(f.toString()));

            // catalog.schema.table/UT_test_column/minmax/UT_test/lastModified=123.tar
            assertEquals(newFiles.size(), 1);

            // all files should be different
            for (Path previousFile : previousFiles) {
                assertFalse(newFiles.contains(previousFile), "should not have found " + previousFile);
            }
        }
    }

    /**
     * create a bloom index followed by a minmax index
     * <p>
     * the lastModifiedTime of the datasource split will remain the same
     * this means both bloom and minmax index should remain with
     * the same lastModifiedFile
     *
     * @throws IOException
     */
    @Test
    public void testIndexWriterMultipleWrites()
            throws IOException
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
                Object[] values = new Object[] {"test", "dsfdfs", "random"};
                callback.call("UT_test_column", values, "UT_test", 100, 123, 0);
            }
        };

        try (TempFolder folder = new TempFolder()) {
            folder.create();

            HetuFileSystemClient fs = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), folder.getRoot().toPath());

            Set<Index> indices = new HashSet<>();
            indices.add(new BloomIndex());
            indices.add(new MinMaxIndex());

            HeuristicIndexWriter writer = new HeuristicIndexWriter(ds, indices, fs, folder.getRoot().toPath());
            String tableName = "catalog.schema.table";

            writer.createIndex(tableName, new String[] {"test"}, new String[] {}, "bloom");

            File indexFolder = new File(folder.getRoot().getAbsolutePath() + "/" + tableName);
            LOG.info("Previous files:");
            Set<Path> previousFiles = Files.walk(Paths.get(indexFolder.getAbsolutePath()))
                    .filter(Files::isRegularFile).collect(Collectors.toSet());
            previousFiles.forEach(f -> LOG.info(f.toString()));

            writer.createIndex(tableName, new String[] {"test"}, new String[] {}, "minmax");
            LOG.info("New files:");
            Set<Path> newFiles = Files.walk(Paths.get(indexFolder.getAbsolutePath()))
                    .filter(Files::isRegularFile).collect(Collectors.toSet());
            newFiles.forEach(f -> LOG.info(f.toString()));

            // two files:
            // catalog.schema.table/UT_test_column/minmax/UT_test/lastModified=123.tar
            // catalog.schema.table/UT_test_column/bloom/UT_test/lastModified=123.tar
            assertEquals(newFiles.size(), 2);
            assertTarEntry(newFiles.iterator().next(), 1);

            // previous files should still be there
            for (Path previousFile : previousFiles) {
                assertTrue(newFiles.contains(previousFile), "did not find " + previousFile);
            }
        }
    }

    /**
     * create a bloom index with debug on, this will also write
     * the data of each split into a file alongside the index file
     *
     * @throws IOException
     */
    @Test
    public void testDebugMode()
            throws IOException
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
                Object[] values = new Object[] {"test", "dsfdfs", "random"};
                callback.call("UT_test_column", values, "UT_test", 100, System.currentTimeMillis(), 0);
            }
        };

        try (TempFolder folder = new TempFolder()) {
            folder.create();

            HetuFileSystemClient fs = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), folder.getRoot().toPath());

            Set<Index> indices = new HashSet<>();
            indices.add(new BloomIndex());

            HeuristicIndexWriter writer = new HeuristicIndexWriter(ds, indices, fs, folder.getRoot().toPath());
            String tableName = "catalog.schema.table";

            writer.createIndex(tableName, new String[] {"test"}, new String[] {}, "bloom");

            File indexFolder = new File(folder.getRoot().getAbsolutePath() + "/" + tableName);
            LOG.info("Previous files:");
            Set<Path> files = Files.walk(Paths.get(indexFolder.getAbsolutePath()))
                    .filter(Files::isRegularFile).collect(Collectors.toSet());
            files.forEach(f -> LOG.info(f.toString()));

            assertEquals(files.size(), 1);
            assertTarEntry(files.iterator().next(), 1);
        }
    }

    private void assertIndexWriterCleanUp(Path root, String tableName)
            throws IOException
    {
        // TODO: note empty directories may be left behind
        assertTrue(Files.walk(Paths.get(root.toAbsolutePath().toString())).noneMatch(Files::isRegularFile),
                "all part files and lock files has to be deleted upon error");
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
            Object[] values = new Object[] {"test", "dsfdfs", "random"};
            callback.call("UT_test_column", values, "UT_test", 100, System.currentTimeMillis(), 0);
        }
    }

    private void assertTarEntry(Path pathToTarFile, int expectedEntryCount)
            throws IOException
    {
        int entryCount = 0;
        try (TarArchiveInputStream i = new TarArchiveInputStream(Files.newInputStream(pathToTarFile))) {
            ArchiveEntry entry;
            while ((entry = i.getNextEntry()) != null) {
                entryCount++;
            }
        }

        assertEquals(entryCount, expectedEntryCount);
    }

    class FilesystemAndRoot
    {
        HetuFileSystemClient fileSystemClient;
        Path root;

        FilesystemAndRoot(HetuFileSystemClient fs, Path root)
        {
            this.fileSystemClient = fs;
            this.root = root;
        }
    }
}
