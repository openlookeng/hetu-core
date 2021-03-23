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
package io.prestosql.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HiveTestUtils;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.Test;

import java.io.File;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.testng.Assert.assertEquals;

public class TestOrcDeleteDeltaPageSource
{
    private static final File DELETE_FILE = new File(TestOrcDeleteDeltaPageSource.class.getClassLoader().getResource("fullacid_delete_delta_test/delete_delta_0000004_0000004_0000/bucket_00000").getPath());

    @Test
    public void testReadingDeletedRows()
    {
        OrcDeleteDeltaPageSourceFactory pageSourceFactory = new OrcDeleteDeltaPageSourceFactory(
                "test",
                new JobConf(new Configuration(false)),
                HiveTestUtils.HDFS_ENVIRONMENT,
                new DataSize(1, MEGABYTE),
                new DataSize(8, MEGABYTE),
                new DataSize(8, MEGABYTE),
                new DataSize(16, MEGABYTE),
                new DataSize(8, MEGABYTE),
                true,
                false,
                new FileFormatDataSourceStats());

        OrcDeleteDeltaPageSource pageSource = pageSourceFactory.createPageSource(new Path(DELETE_FILE.toURI()), DELETE_FILE.length(), DELETE_FILE.lastModified());
        MaterializedResult materializedRows = MaterializedResult.materializeSourceDataStream(HiveTestUtils.SESSION, pageSource, ImmutableList.of(BIGINT, INTEGER, BIGINT));

        assertEquals(materializedRows.getRowCount(), 1);
        assertEquals(materializedRows.getMaterializedRows().get(0), new MaterializedRow(5, 2L, 536870912, 0L));
    }
}
