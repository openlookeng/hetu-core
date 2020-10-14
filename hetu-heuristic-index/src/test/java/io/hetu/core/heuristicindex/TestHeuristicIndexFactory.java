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
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
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

public class TestHeuristicIndexFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(TestHeuristicIndexFactory.class);

    private String datastoreTypeKey = "connector.name";
    private String emptyDataSourceId = "empty";

    @Test
    public void testCsvDataSourceLocalIndexStore()
            throws IOException
    {
        Properties dataSourceProps = new Properties();
        File csvResources = new File("src/test/resources/csv").getCanonicalFile();
        dataSourceProps.setProperty("connector.name", "csv");
        dataSourceProps.setProperty("rootDir", csvResources.getAbsolutePath());
        DataSource dataSource = new CsvDataSource();
        dataSource.setProperties(dataSourceProps);

        try (TempFolder folder = new TempFolder()) {
            folder.create();
            Set<Index> indexes = new HashSet<>();
            indexes.add(new MinMaxIndex());
            indexes.add(new BloomIndex());

            HetuFileSystemClient fs = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), folder.getRoot().toPath());

            HeuristicIndexWriter writer = new HeuristicIndexWriter(dataSource, indexes, fs, folder.getRoot().toPath());

            String table = "csv.schemaName.tableName";
            String[] columns = new String[] {"0", "2"};
            String[] partitons = new String[] {"p=bar"};
            writer.createIndex(table, columns, partitons, "bloom");
            writer.createIndex(table, columns, partitons, "minmax");

            IndexClient client = new HeuristicIndexFactory().getIndexClient(fs, folder.getRoot().toPath());
            List<IndexMetadata> splits = client.readSplitIndex(table);
            // there should be 8 splits (2 csv files * 2 columns * 2 index types)
            assertEquals(8, splits.size());

            // read the index file for csv/schemaName/tableName/p=bar/000.csv, column 2
            File csvFile = new File("src/test/resources/csv/schemaName/tableName/p=bar/000.csv").getCanonicalFile();
            String filePathBloom = Paths.get("csv.schemaName.tableName", "2", "bloom", csvFile.toString()).toString();
            String filePathMinMax = Paths.get("csv.schemaName.tableName", "2", "minmax", csvFile.toString()).toString();
            splits = client.readSplitIndex(filePathBloom);
            // Bloom only
            assertEquals(1, splits.size());
            splits.addAll(client.readSplitIndex(filePathMinMax));
            // Bloom and minmax
            assertEquals(2, splits.size());
            BloomIndex bloomIndex = null;
            MinMaxIndex minMaxIndex = null;
            for (IndexMetadata split : splits) {
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

            assertTrue(bloomIndex.matches(new SqlParser().createExpression("(2 = 22)", new ParsingOptions())));
            assertFalse(bloomIndex.matches(new SqlParser().createExpression("(2 = 99)", new ParsingOptions())));

            assertTrue(minMaxIndex.matches(new SqlParser().createExpression("(2 < 25)", new ParsingOptions())));
            assertFalse(minMaxIndex.matches(new SqlParser().createExpression("(2 < 15)", new ParsingOptions())));
        }
    }

    @Test
    public void testLoadDataSourceProperties()
    {
        // Null check
        assertThrows(NullPointerException.class, () -> new HeuristicIndexFactory().getIndexWriter((Properties) null, null, null, null));

        Properties dsProps = new Properties();
        Properties ixProps = new Properties();
        Path root = Paths.get("/tmp");
        HetuFileSystemClient fs = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), root);

        // Empty data source type
        dsProps.setProperty(datastoreTypeKey, "");
        assertThrows(RuntimeException.class, () -> new HeuristicIndexFactory().getIndexWriter(dsProps, ixProps, fs, root));

        // loading empty data source
        dsProps.setProperty(datastoreTypeKey, emptyDataSourceId);
        assertNotNull(new HeuristicIndexFactory().getIndexWriter(dsProps, ixProps, fs, root));
    }
}
