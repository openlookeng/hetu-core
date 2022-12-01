/*
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
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.prestosql.orc.OrcTester.Format;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.StripeInformation;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.OrcConf;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.orc.OrcTester.Format.ORC_12;
import static io.prestosql.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.prestosql.orc.OrcTester.writeOrcFileColumnHive;
import static io.prestosql.orc.metadata.CompressionKind.NONE;
import static io.prestosql.orc.metadata.CompressionKind.ZLIB;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.testng.Assert.assertEquals;

public class TestOrcRecordReader
{
    private static final int POSITION_COUNT = 50000;

    private TempFile tempFile;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        tempFile = new TempFile();
        SecureRandom random = new SecureRandom();
        Iterator<String> iterator = Stream.generate(() -> Long.toHexString(random.nextLong())).limit(POSITION_COUNT).iterator();
        writeOrcFileColumnHive(
                tempFile.getFile(),
                createOrcRecordWriter(tempFile.getFile(), ORC_12, ZLIB, javaStringObjectInspector),
                VARCHAR,
                iterator);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        tempFile.close();
    }

    @Test
    public void testSmoke1()
            throws IOException
    {
        TestingOrcDataSource orcDataSource = new TestingOrcDataSource(
                new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), true, tempFile.getFile().lastModified()));
        createOrcReader(orcDataSource, new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE));
        assertEquals(orcDataSource.getReadCount(), 1);

        orcDataSource = new TestingOrcDataSource(
                new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), true, tempFile.getFile().lastModified()));
        createOrcReader(orcDataSource, new DataSize(400, Unit.KILOBYTE), new DataSize(400, Unit.KILOBYTE));
        assertEquals(orcDataSource.getReadCount(), 3);
    }

    private void createOrcReader(TestingOrcDataSource orcDataSource, DataSize maxMergeDistance, DataSize tinyStripeThreshold)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(orcDataSource, maxMergeDistance, tinyStripeThreshold, new DataSize(1, Unit.MEGABYTE));
        assertEquals(orcDataSource.getReadCount(), 1);
        List<StripeInformation> stripes = orcReader.getFooter().getStripes();
        assertGreaterThanOrEqual(stripes.size(), 3);

        OrcRecordReader orcRecordReader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(VARCHAR),
                (numberOfRows, statisticsByColumnIndex) -> true,
                HIVE_STORAGE_TIME_ZONE,
                newSimpleAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE,
                RuntimeException::new);
        int positionCount = 0;
        while (true) {
            Page page = orcRecordReader.nextPage();
            if (page == null) {
                break;
            }
            page = page.getLoadedPage();
            Block block = page.getBlock(0);
            positionCount += block.getPositionCount();
        }
        assertEquals(positionCount, POSITION_COUNT);

        assertEquals(orcRecordReader.stripeInfos.size(), orcRecordReader.stripes.size());
    }

    private static FileSinkOperator.RecordWriter createOrcRecordWriter(File outputFile, Format format, CompressionKind compression, ObjectInspector columnObjectInspector)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        OrcConf.WRITE_FORMAT.setString(jobConf, format == ORC_12 ? "0.12" : "0.11");
        OrcConf.COMPRESS.setString(jobConf, compression.name());

        Properties tableProperties = new Properties();
        tableProperties.setProperty(IOConstants.COLUMNS, "test");
        tableProperties.setProperty(IOConstants.COLUMNS_TYPES, columnObjectInspector.getTypeName());
        tableProperties.setProperty(OrcConf.STRIPE_SIZE.getAttribute(), "120000");

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compression != NONE,
                tableProperties,
                () -> {});
    }
}
