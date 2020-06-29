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
package io.hetu.core.heuristicindex.hive;

//import io.airlift.units.DataSize;
//import io.hetu.core.orc.OrcDataSource;
//import io.hetu.core.orc.OrcDataSourceId;
//import io.hetu.core.orc.OrcReader;
//import io.hetu.core.plugin.hive.FileFormatDataSourceStats;
//import io.hetu.core.plugin.hive.orc.HdfsOrcDataSource;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.testng.annotations.AfterTest;
//import org.testng.annotations.BeforeTest;
//import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

//import java.io.IOException;
//import java.math.BigDecimal;
//import java.time.LocalDate;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Properties;
//import java.util.concurrent.ConcurrentHashMap;
//
//import static io.airlift.units.DataSize.Unit.MEGABYTE;
//import static org.testng.Assert.assertEquals;

@Test(enabled = false)
public class TestHiveDataSource
{
//    private static final Logger LOG = LoggerFactory.getLogger(TestHiveDataSource.class);
//
//    private final DockerizedHive container = DockerizedHive.getInstance();
//
//    private String databaseName;
//    private String tableName;
//    private String tableUrl;
//
//    private HiveDataSource testSource;
//
//    String[] partitions;
//
//    static class Counter
//    {
//        long value;
//
//        public void increment()
//        {
//            ++value;
//        }
//
//        public void add(int num)
//        {
//            value += num;
//        }
//    }
//
//    public TestHiveDataSource() throws IOException
//    {
//        Properties properties = container.getHiveProperties();
//        testSource = new HiveDataSource(properties);
//    }
//
//    @BeforeTest
//    public void prepare()
//    {
//        this.databaseName = DockerizedHive.DATABASE_NAME;
//        this.tableName = DockerizedHive.TABLE_NAME;
//    }
//
//    @Test
//    public void testRowNumberForSplitGenerationWithoutPropertiesGiven() throws IOException
//    {
//        setTableUrl(DockerizedHive.DATABASE_NAME, DockerizedHive.TABLE_NAME, null);
//
//        HiveDataSource source = new HiveDataSource();
//        Properties properties = container.getHiveProperties();
//        source.setProperties(properties);
//
//        rowNumberForSplitGeneration(source, new String[]{"name", "age"}, 2);
//    }
//
//    @Test
//    public void testRowNumberForSplitGeneration() throws IOException
//    {
//        setTableUrl(DockerizedHive.DATABASE_NAME, DockerizedHive.TABLE_NAME, null);
//
//        rowNumberForSplitGeneration(testSource, new String[]{"name"}, 1);
//    }
//
//    @Test
//    public void testRowNumberForSplitGenerationUnderNoPartition() throws IOException
//    {
//        String[] partitions = new String[]{"111OOOJJJbbbTTTQQQ"};
//        setTableUrl(DockerizedHive.DATABASE_NAME, DockerizedHive.TABLE_NAME, partitions);
//
//        // No partition in this table, this should return 0
//        assertEquals(0, rowNumberForSplitGeneration(testSource, new String[]{"name", "age"}, 2));
//    }
//
//    @Test
//    public void testRowNumberForSplitGenerationUnderPartition() throws IOException
//    {
//        String[] partitions = new String[]{"age=19", "age=30"};
//        setTableUrl(DockerizedHive.DATABASE_NAME, DockerizedHive.PARTITIONED_TABLE_NAME, partitions);
//
//        rowNumberForSplitGeneration(testSource, new String[]{"name"}, 1);
//    }
//
//    @Test
//    public void testSupportedColumnTypes() throws IOException
//    {
//        Map<String, Class> expectedColumnClasses = new HashMap<>();
//        expectedColumnClasses.put("t_string", String.class);
//        expectedColumnClasses.put("t_tinyint", Byte.class);
//        expectedColumnClasses.put("t_smallint", Short.class);
//        expectedColumnClasses.put("t_int", Integer.class);
//        expectedColumnClasses.put("t_bigint", Long.class);
//        expectedColumnClasses.put("t_float", Float.class);
//        expectedColumnClasses.put("t_double", Double.class);
//        expectedColumnClasses.put("t_decimal", BigDecimal.class);
//        expectedColumnClasses.put("t_boolean", Boolean.class);
//        expectedColumnClasses.put("t_varchar", String.class);
//        expectedColumnClasses.put("t_char", String.class);
//        expectedColumnClasses.put("t_date", LocalDate.class);
//
//        Map<String, Class> readColumnClasses = new ConcurrentHashMap<>();
//
//        testSource.readSplits(DockerizedHive.DATABASE_NAME, DockerizedHive.TYPES_TABLE_NAME, expectedColumnClasses.keySet().toArray(new String[0]), null,
//                (column, values, uri, splitStart, lastModified) -> readColumnClasses.putIfAbsent(column, values[0].getClass()));
//
//        assertEquals(readColumnClasses, expectedColumnClasses);
//    }
//
//    @DataProvider(name = "testUnsupportedColumnTypes")
//    public Object[][] getUnsupportedColumns()
//    {
//        return new Object[][]{{"t_row"}, {"t_timestamp"}, {"t_binary"}, {"t_lables"}, {"t_array"}};
//    }
//
//    @Test(dataProvider = "testUnsupportedColumnTypes", expectedExceptions = IllegalArgumentException.class)
//    public void testUnsupportedColumnTypes(String unsupportedColumn) throws IOException
//    {
//        Map<String, Class> readColumnClasses = new ConcurrentHashMap<>();
//
//        testSource.readSplits(DockerizedHive.DATABASE_NAME, DockerizedHive.TYPES_TABLE_NAME, new String[]{unsupportedColumn}, null,
//                (column, values, uri, splitStart, lastModified) -> readColumnClasses.putIfAbsent(column, values[0].getClass()));
//    }
//
//    @AfterTest
//    public void cleanUp()
//    {
//        container.close();
//    }
//
//    private long rowNumberForSplitGeneration(HiveDataSource source, String[] columns, int effectiveColumnCount) throws IOException
//    {
//        Counter splitCounter = new Counter();
//        Counter rowCounter = new Counter();
//
//        Configuration conf = container.getHadoopConfiguration();
//
//        FileSystem fs = FileSystem.get(conf);
//        long totalRows = getTotalRows(fs, tableUrl);
//
//        source.readSplits(databaseName, tableName, columns, partitions, (column, values, uri, splitStart, lastModified) -> {
//            rowCounter.add(values.length);
//            splitCounter.increment();
//        });
//
//        long counterValue = splitCounter.value;
//        long dataSize = rowCounter.value;
//
//        assertEquals(dataSize / effectiveColumnCount, totalRows);
//        LOG.info("Splits created: " + counterValue);
//
//        return counterValue;
//    }
//
//    private long getTotalRows(FileSystem fs, String tableDir) throws IOException
//    {
//        Path tablePath = new Path(tableDir);
//        long totalRows = 0;
//        for (FileStatus fileStatus : HadoopUtil.getFiles(fs, tablePath, partitions, false)) {
//            OrcReader orcReader = new OrcReader(getHDFSOrcDataSource(fileStatus.getPath().toString()), new DataSize(1, DataSize.Unit.MEGABYTE),
//                    new DataSize(1, DataSize.Unit.MEGABYTE), new DataSize(1, DataSize.Unit.MEGABYTE));
//            totalRows += orcReader.getFooter().getNumberOfRows();
//        }
//        return totalRows;
//    }
//
//    private void setTableUrl(String databaseName, String tableName, String[] partitions)
//    {
//        this.databaseName = databaseName;
//        this.tableName = tableName;
//        this.partitions = partitions;
//
//        this.tableUrl = String.format("/user/hive/warehouse/%s.db/%s", databaseName, tableName);
//    }
//
//    private OrcDataSource getHDFSOrcDataSource(String orcFilePathStr) throws IOException
//    {
//        Configuration config = container.getHadoopConfiguration();
//        Path orcFilePath = new Path(orcFilePathStr);
//
//        FileSystem fs = FileSystem.get(config);
//        FSDataInputStream in = fs.open(orcFilePath);
//        FileStatus status = fs.getFileStatus(orcFilePath);
//
//        return new HdfsOrcDataSource(new OrcDataSourceId(orcFilePathStr), status.getLen(),
//                new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE),
//                true, in, new FileFormatDataSourceStats());
//    }
}
