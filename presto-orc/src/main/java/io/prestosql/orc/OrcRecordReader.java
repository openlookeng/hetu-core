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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.MetadataReader;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.metadata.PostScript.HiveWriterVersion;
import io.prestosql.orc.metadata.StripeInformation;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.orc.metadata.statistics.StripeStatistics;
import io.prestosql.orc.reader.ColumnReader;
import io.prestosql.orc.reader.ColumnReaders;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.SplitMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.prestosql.orc.reader.ColumnReaders.createColumnReader;
import static io.prestosql.spi.HetuConstant.DATASOURCE_FILE_MODIFICATION;
import static io.prestosql.spi.HetuConstant.DATASOURCE_FILE_PATH;
import static io.prestosql.spi.HetuConstant.DATASOURCE_INDEX_LEVEL;
import static io.prestosql.spi.HetuConstant.DATASOURCE_PAGE_NUMBER;
import static io.prestosql.spi.HetuConstant.DATASOURCE_STRIPE_LENGTH;
import static io.prestosql.spi.HetuConstant.DATASOURCE_STRIPE_NUMBER;
import static io.prestosql.spi.HetuConstant.DATASOURCE_STRIPE_OFFSET;
import static io.prestosql.spi.HetuConstant.DATASOURCE_TOTAL_PAGES;
import static java.lang.Math.toIntExact;

public class OrcRecordReader
        extends AbstractOrcRecordReader<ColumnReader>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcRecordReader.class).instanceSize();
    private static final Logger log = Logger.get(OrcRecordReader.class);

    private SplitMetadata splitMetadata;
    private int pageCount;

    public OrcRecordReader(
            List<OrcColumn> readColumns,
            List<Type> readTypes,
            OrcPredicate predicate,
            long numberOfRows,
            List<StripeInformation> fileStripes,
            Optional<ColumnMetadata<ColumnStatistics>> fileStats,
            List<Optional<StripeStatistics>> stripeStats,
            OrcDataSource orcDataSource,
            long splitOffset,
            long splitLength,
            ColumnMetadata<OrcType> orcTypes,
            Optional<OrcDecompressor> decompressor,
            int rowsInRowGroup,
            DateTimeZone hiveStorageTimeZone,
            HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            Map<String, Slice> userMetadata,
            AggregatedMemoryContext systemMemoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize,
            Function<Exception, RuntimeException> exceptionTransform,
            Optional<List<IndexMetadata>> indexes,
            SplitMetadata splitMetadata,
            Map<String, Domain> domains,
            OrcCacheStore orcCacheStore,
            OrcCacheProperties orcCacheProperties,
            boolean pageMetadataEnabled)
            throws OrcCorruptionException
    {
        super(readColumns,
                readTypes,
                predicate,
                numberOfRows,
                fileStripes,
                fileStats,
                stripeStats,
                orcDataSource,
                splitOffset,
                splitLength,
                orcTypes,
                decompressor,
                rowsInRowGroup,
                hiveStorageTimeZone,
                hiveWriterVersion,
                metadataReader,
                maxMergeDistance,
                tinyStripeThreshold,
                maxBlockSize,
                userMetadata,
                systemMemoryUsage,
                writeValidation,
                initialBatchSize,
                exceptionTransform,
                indexes,
                domains,
                orcCacheStore,
                orcCacheProperties,
                ImmutableMap.of(),
                pageMetadataEnabled);
        this.splitMetadata = splitMetadata;

        setColumnReadersParam(createColumnReaders(readColumns, readTypes,
                systemMemoryUsage.newAggregatedMemoryContext(),
                new OrcBlockFactory(exceptionTransform, true),
                orcCacheStore, orcCacheProperties));
    }

    public Page nextPage()
            throws IOException
    {
        ColumnReader[] columnsReader = getColumnReaders();
        int batchSize = prepareNextBatch();
        if (batchSize < 0) {
            return null;
        }

        for (ColumnReader column : columnsReader) {
            if (column != null) {
                column.prepareNextRead(batchSize);
            }
        }
        batchRead(batchSize);

        matchingRowsInBatchArray = null;

        validateWritePageChecksum(batchSize);

        // create a lazy page
        blockFactory.nextPage();
        Arrays.fill(currentBytesPerCell, 0);
        Block[] blocks = new Block[columnsReader.length];
        for (int i = 0; i < columnsReader.length; i++) {
            int columnIndex = i;
            blocks[columnIndex] = blockFactory.createBlock(
                    batchSize,
                    () -> filterRows(columnsReader[columnIndex].readBlock()),
                    block -> blockLoaded(columnIndex, block));
        }

        // only include page metadata if enabled
        if (pageMetadataEnabled) {
            Properties pageMetadata = new Properties();
            pageCount++;
            pageMetadata.setProperty(DATASOURCE_PAGE_NUMBER, String.valueOf(pageCount));
            if (isCurrentStripeFinished()) {
                // Only set the total page count when the current stripe has finished
                // Therefore whenever this property is available in pageMetaData,
                // it indicates that the stripe has finished and this is the last page
                pageMetadata.setProperty(DATASOURCE_TOTAL_PAGES, String.valueOf(pageCount));
                pageCount = 0;
            }
            pageMetadata.setProperty(DATASOURCE_STRIPE_NUMBER, String.valueOf(currentStripe));
            pageMetadata.setProperty(DATASOURCE_STRIPE_OFFSET, String.valueOf(stripes.get(currentStripe).getOffset()));
            pageMetadata.setProperty(DATASOURCE_STRIPE_LENGTH, String.valueOf(stripes.get(currentStripe).getTotalLength()));
            if (splitMetadata != null) {
                // Skip setting for testing (splitMetadata set as null)
                pageMetadata.setProperty(DATASOURCE_FILE_PATH, splitMetadata.getSplitIdentity());
                pageMetadata.setProperty(DATASOURCE_FILE_MODIFICATION, String.valueOf(splitMetadata.getLastModifiedTime()));
            }
            pageMetadata.setProperty(DATASOURCE_INDEX_LEVEL, "STRIPE");
            return new Page(batchSize, pageMetadata, blocks);
        }
        else {
            return new Page(batchSize, blocks);
        }
    }

    private Block filterRows(Block block)
    {
        // currentPosition to currentBatchSize
        StripeInformation stripe = stripes.get(currentStripe);

        if (matchingRowsInBatchArray == null && stripeMatchingRows.containsKey(
                stripe) && block.getPositionCount() != 0) {
            long currentPositionInStripe = currentPosition - currentStripePosition;

            PeekingIterator<Integer> matchingRows = stripeMatchingRows.get(stripe);
            List<Integer> matchingRowsInBlock = new ArrayList<>();

            while (matchingRows.hasNext()) {
                Integer row = matchingRows.peek();
                if (row >= currentPositionInStripe && row < currentPositionInStripe + currentBatchSize) {
                    matchingRowsInBlock.add(toIntExact(Long.valueOf(row) - currentPositionInStripe));
                    matchingRows.next();
                }
                else if (row >= currentPositionInStripe + currentBatchSize) {
                    break;
                }
            }

            matchingRowsInBatchArray = new int[matchingRowsInBlock.size()];
            IntStream.range(0, matchingRowsInBlock.size()).forEach(
                    i -> matchingRowsInBatchArray[i] = matchingRowsInBlock.get(i));
            log.debug("Find matching rows from stripe. Matching row count for the block = %d", matchingRowsInBatchArray.length);
        }

        if (matchingRowsInBatchArray != null) {
            return block.copyPositions(matchingRowsInBatchArray, 0, matchingRowsInBatchArray.length);
        }

        return block;
    }

    public Map<String, Slice> getUserMetadata()
    {
        return ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slices::copyOf));
    }

    /**
     * @return The total size of memory retained by this OrcRecordReader
     */
    @VisibleForTesting
    protected long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + super.getRetainedSizeInBytes();
    }

    private void validateWritePageChecksum(int batchSize)
            throws IOException
    {
        if (writeChecksumBuilder.isPresent()) {
            ColumnReader[] columnsReader = getColumnReaders();
            Block[] blocks = new Block[columnsReader.length];
            for (int columnIndex = 0; columnIndex < columnsReader.length; columnIndex++) {
                Block block = columnsReader[columnIndex].readBlock();
                blocks[columnIndex] = block;
                blockLoaded(columnIndex, block);
            }

            Page page = new Page(batchSize, blocks);
            validateWritePageChecksum(page);
        }
    }

    private ColumnReader[] createColumnReaders(
            List<OrcColumn> columns,
            List<Type> readTypes,
            AggregatedMemoryContext systemMemoryContext,
            OrcBlockFactory blockFactory,
            OrcCacheStore orcCacheStore,
            OrcCacheProperties orcCacheProperties)
            throws OrcCorruptionException
    {
        ColumnReader[] columnReaders = new ColumnReader[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            int columnIndex = i;
            Type readType = readTypes.get(columnIndex);
            OrcColumn column = columns.get(columnIndex);
            ColumnReader columnReader = createColumnReader(
                    readType,
                    column,
                    systemMemoryContext,
                    blockFactory.createNestedBlockFactory(block -> blockLoaded(columnIndex, block)));
            if (orcCacheProperties.isRowDataCacheEnabled()) {
                columnReader = ColumnReaders.wrapWithCachingStreamReader(columnReader, column, orcCacheStore.getRowDataCache());
            }
            columnReaders[columnIndex] = columnReader;
        }
        return columnReaders;
    }
}
