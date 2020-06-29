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

import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HiveTestUtils;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.testing.MaterializedResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.testng.Assert.assertEquals;

public class TestOrcDeletedRows
{
    private Path partitionDirectory;
    private Block bucketBlock;
    private Block rowIdBlock;

    @BeforeClass
    public void setUp()
    {
        partitionDirectory = new Path(TestOrcDeletedRows.class.getClassLoader().getResource("fullacid_delete_delta_test") + "/");
        bucketBlock = INTEGER.createFixedSizeBlockBuilder(1)
            .writeInt(536870912)
            .build();
        rowIdBlock = BIGINT.createFixedSizeBlockBuilder(1)
            .writeLong(0)
            .build();
    }

    @Test
    public void testEmptyDeleteLocations()
    {
        OrcDeletedRows deletedRows = createOrcDeletedRows(Optional.empty());

        Page testPage = createTestPage(0, 10);
        Block block = deletedRows.getMaskDeletedRowsFunction(testPage, Optional.empty()).apply(testPage.getBlock(2));
        assertEquals(block.getPositionCount(), 10);
    }

    @Test
    public void testDeleteLocations()
    {
        DeleteDeltaLocations.Builder deleteDeltaLocationsBuilder = DeleteDeltaLocations.builder(partitionDirectory);
        addDeleteDelta(deleteDeltaLocationsBuilder, 4L, 4L, 0);
        addDeleteDelta(deleteDeltaLocationsBuilder, 7L, 7L, 0);

        OrcDeletedRows deletedRows = createOrcDeletedRows(deleteDeltaLocationsBuilder.build());

        // page with deleted rows
        Page testPage = createTestPage(0, 10);
        Block block = deletedRows.getMaskDeletedRowsFunction(testPage, Optional.empty()).apply(testPage.getBlock(0));
        Set<Object> validRows = MaterializedResult.resultBuilder(HiveTestUtils.SESSION, BIGINT)
                .page(new Page(block))
                .build()
                .getOnlyColumnAsSet();

        assertEquals(validRows.size(), 8);
        assertEquals(validRows, ImmutableSet.of(0L, 1L, 3L, 4L, 5L, 7L, 8L, 9L));

        // page with no deleted rows
        testPage = createTestPage(10, 20);
        block = deletedRows.getMaskDeletedRowsFunction(testPage, Optional.empty()).apply(testPage.getBlock(2));
        assertEquals(block.getPositionCount(), 10);
    }

    private void addDeleteDelta(DeleteDeltaLocations.Builder deleteDeltaLocationsBuilder, long minWriteId, long maxWriteId, int statementId)
    {
        Path deleteDeltaPath = new Path(partitionDirectory, AcidUtils.deleteDeltaSubdir(minWriteId, maxWriteId, statementId));
        deleteDeltaLocationsBuilder.addDeleteDelta(deleteDeltaPath, minWriteId, maxWriteId, statementId);
    }

    private OrcDeletedRows createOrcDeletedRows(Optional<DeleteDeltaLocations> deleteDeltaLocations)
    {
        JobConf configuration = new JobConf(new Configuration(false));
        OrcDeleteDeltaPageSourceFactory pageSourceFactory = new OrcDeleteDeltaPageSourceFactory(
                "test",
                configuration,
                HiveTestUtils.HDFS_ENVIRONMENT,
                new DataSize(1, MEGABYTE),
                new DataSize(8, MEGABYTE),
                new DataSize(8, MEGABYTE),
                new DataSize(16, MEGABYTE),
                new DataSize(8, MEGABYTE),
                true,
                false,
                new FileFormatDataSourceStats());

        return new OrcDeletedRows(
                "bucket_00000",
                deleteDeltaLocations,
                pageSourceFactory,
                "test",
                configuration,
                HiveTestUtils.HDFS_ENVIRONMENT,
                Optional.empty());
    }

    private Page createTestPage(int originalTransactionStart, int originalTransactionEnd)
    {
        int size = originalTransactionEnd - originalTransactionStart;
        BlockBuilder originalTransaction = BIGINT.createFixedSizeBlockBuilder(size);
        for (long i = originalTransactionStart; i < originalTransactionEnd; i++) {
            originalTransaction.writeLong(i);
        }

        return new Page(
                size,
                originalTransaction.build(),
                new RunLengthEncodedBlock(bucketBlock, size),
                new RunLengthEncodedBlock(rowIdBlock, size));
    }
}
