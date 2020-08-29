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

import com.google.common.collect.PeekingIterator;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.MetadataReader;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.metadata.PostScript;
import io.prestosql.orc.metadata.StripeInformation;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.orc.metadata.statistics.StripeStatistics;
import io.prestosql.orc.reader.ColumnReader;
import io.prestosql.orc.reader.ColumnReaders;
import io.prestosql.orc.reader.SelectiveColumnReader;
import io.prestosql.orc.reader.SelectiveColumnReaders;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.orc.reader.SelectiveColumnReaders.createColumnReader;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class OrcSelectiveRecordReader
        extends AbstractOrcRecordReader<SelectiveColumnReader>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcSelectiveRecordReader.class).instanceSize();
    private static final Logger log = Logger.get(OrcSelectiveRecordReader.class);

    private final List<Integer> excludePositions;
    private final List<Integer> columnReaderOrder;
    private final Map<Integer, TupleDomainFilter> filters;
    private int[] positions;
    List<Integer> outputColumns;
    Map<Integer, Type> includedColumns;

    private final Map<Integer, Object> constantValues;
    Set<Integer> colReaderWithFilter;
    Set<Integer> colReaderWithORFilter;
    Set<Integer> colReaderWithoutFilter;
    Map<Integer, List<TupleDomainFilter>> additionalFilters;
    Map<Integer, Function<Block, Block>> coercers;

    public OrcSelectiveRecordReader(
            List<Integer> outputColumns,
            Map<Integer, Type> includedColumns,
            List<OrcColumn> fileColumns,
            List<OrcColumn> fileReadColumns,
            List<Type> readTypes,
            Map<Integer, TupleDomainFilter> filters,
            Map<Integer, Object> constantValues,
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
            PostScript.HiveWriterVersion hiveWriterVersion,
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
            Map<String, Domain> domains,
            OrcCacheStore orcCacheStore,
            OrcCacheProperties orcCacheProperties,
            Map<Integer, List<TupleDomainFilter>> additionalFilters,
            List<Integer> positions,
            boolean useDataCache,
            Map<Integer, Function<Block, Block>> coercers,
            Map<String, List<Domain>> orDomains)
            throws OrcCorruptionException
    {
        super(fileReadColumns,
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
                orDomains);

        int fieldCount = orcTypes.get(OrcColumnId.ROOT_COLUMN).getFieldCount();
        this.columnReaderOrder = new ArrayList<>(fieldCount);

        this.outputColumns = outputColumns;
        this.includedColumns = includedColumns;
        this.excludePositions = positions;

        this.filters = filters;
        this.additionalFilters = additionalFilters;
        this.constantValues = requireNonNull(constantValues, "constantValues is null");
        this.coercers = requireNonNull(coercers, "coercers is null");

        for (Map.Entry<Integer, Function<Block, Block>> entry : coercers.entrySet()) {
            checkArgument(!filters.containsKey(entry.getKey()), "Coercions for columns with range filters are not yet supported");
        }

        setColumnReadersParam(createColumnReaders(fileColumns,
                systemMemoryUsage.newAggregatedMemoryContext(),
                new OrcBlockFactory(exceptionTransform, true),
                orcCacheStore, orcCacheProperties,
                predicate, filters, hiveStorageTimeZone,
                outputColumns, includedColumns, orcTypes, useDataCache));
    }

    public Page getNextPage()
            throws IOException
    {
        int batchSize = prepareNextBatch();
        if (batchSize < 0) {
            return null;
        }

        matchingRowsInBatchArray = null;
        initializePositions(batchSize);

        int[] positionsToRead = this.positions;
        int positionCount = batchSize;

        /* first evaluate columns with filter AND conditions */
        SelectiveColumnReader[] columnReaders = getColumnReaders();
        if (positionCount != 0) {
            for (Integer columnIdx : colReaderWithFilter) {
                if (columnReaders[columnIdx] != null) {
                    positionCount = columnReaders[columnIdx].read(getNextRowInGroup(), positionsToRead, positionCount, filters.get(columnIdx));
                    if (positionCount == 0) {
                        break;
                    }

                    // Get list of row position to read for the next column. Output of positions from current column is
                    // input to the next column
                    positionsToRead = columnReaders[columnIdx].getReadPositions();
                }
            }
        }

        /* perform OR filtering */
        BitSet accumulator = new BitSet();
        if (colReaderWithORFilter.size() > 0 && positionCount > 0) {
            int localPositionCount = positionCount;
            for (Integer columnIdx : colReaderWithORFilter) {
                if (columnReaders[columnIdx] != null) {
                    localPositionCount += columnReaders[columnIdx].readOr(getNextRowInGroup(), positionsToRead, positionCount, additionalFilters.get(columnIdx), accumulator);
                }
            }

            int[] newPositions = positionsToRead.clone();
            positionCount = updateExcludePositions(positionsToRead, positionCount, accumulator, newPositions);
            positionsToRead = Arrays.copyOf(newPositions, positionCount);
        }

        if (positionCount != 0) {
            for (Integer columnIdx : colReaderWithoutFilter) {
                if (columnReaders[columnIdx] != null) {
                    positionCount = columnReaders[columnIdx].read(getNextRowInGroup(), positionsToRead, positionCount, null);
                    if (positionCount == 0) {
                        break;
                    }

                    // Get list of row position to read for the next column. Output of positions from current column is
                    // input to the next column
                    positionsToRead = columnReaders[columnIdx].getReadPositions();
                }
            }
        }

        batchRead(batchSize);

        if (positionCount == 0) {
            return new Page(0);
        }

        // Finally this makes block of all output columns in the page. Row positions list from the last column are the
        // final positions list after applying all projection. Only that final row position will be used to get data of
        // all columns and form the page block.
        Block[] blocks = new Block[outputColumns.size()];
        for (int i = 0; i < outputColumns.size(); i++) {
            int columnIndex = outputColumns.get(i);
            if (columnIndex < 0) {
                // To fill partition key.
                blocks[i] = RunLengthEncodedBlock.create(includedColumns.get(columnIndex), constantValues.get(columnIndex), positionCount);
            }
            else {
                Block block = getColumnReaders()[columnIndex].getBlock(positionsToRead, positionCount);
                updateMaxCombinedBytesPerRow(columnIndex, block);
                if (coercers.containsKey(i)) {
                    block = coercers.get(i).apply(block);
                }
                blocks[i] = block;
            }
        }

        Page page = new Page(positionCount, blocks);

        validateWritePageChecksum(page);

        return page;
    }

    private void initializePositions(int batchSize)
    {
        // currentPosition to currentBatchSize
        StripeInformation stripe = stripes.get(currentStripe);

        if (matchingRowsInBatchArray == null && stripeMatchingRows.containsKey(stripe)) {
            long currentPositionInStripe = currentPosition - currentStripePosition;

            PeekingIterator<Integer> matchingRows = stripeMatchingRows.get(stripe);
            List<Integer> matchingRowsInBlock = new ArrayList<>();

            while (matchingRows.hasNext()) {
                Integer row = matchingRows.peek();
                if (row >= currentPositionInStripe && row < currentPositionInStripe + batchSize) {
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

        if (positions == null || positions.length < batchSize) {
            if (matchingRowsInBatchArray != null) {
                positions = matchingRowsInBatchArray;
                return;
            }

            positions = new int[batchSize];
            for (int i = 0; i < batchSize; i++) {
                positions[i] = i;
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        super.close();
    }

    private int updateExcludePositions(int[] positions, int positionCount, BitSet accumulator, int[] newPositions)
    {
        int totalPositions = 0;
        for (int i = 0; i < positionCount; i++) {
            if (accumulator.get(positions[i])) {
                newPositions[totalPositions++] = positions[i];
            }
        }

        return totalPositions;
    }

    public SelectiveColumnReader[] createColumnReaders(
            List<OrcColumn> fileColumns,
            AggregatedMemoryContext systemMemoryContext,
            OrcBlockFactory blockFactory,
            OrcCacheStore orcCacheStore,
            OrcCacheProperties orcCacheProperties,
            OrcPredicate predicate,
            Map<Integer, TupleDomainFilter> filters,
            DateTimeZone hiveStorageTimeZone,
            List<Integer> outputColumns,
            Map<Integer, Type> includedColumns,
            ColumnMetadata<OrcType> orcTypes,
            boolean useDataCache)
            throws OrcCorruptionException
    {
        int fieldCount = orcTypes.get(OrcColumnId.ROOT_COLUMN).getFieldCount();
        SelectiveColumnReader[] columnReaders = new SelectiveColumnReader[fieldCount];

        colReaderWithFilter = new IntArraySet();
        colReaderWithORFilter = new IntArraySet();
        colReaderWithoutFilter = new IntArraySet();
        for (int i = 0; i < fieldCount; i++) {
            if (includedColumns.containsKey(i)) {
                int columnIndex = i;
                OrcColumn column = fileColumns.get(columnIndex);
                boolean outputRequired = outputColumns.contains(i);
                SelectiveColumnReader columnReader = null;

                if (useDataCache && orcCacheProperties.isRowDataCacheEnabled()) {
                    ColumnReader cr = ColumnReaders.createColumnReader(
                            includedColumns.get(i),
                            column,
                            systemMemoryContext,
                            blockFactory.createNestedBlockFactory(block -> blockLoaded(columnIndex, block)));
                    columnReader = SelectiveColumnReaders.wrapWithDataCachingStreamReader(cr, column, orcCacheStore.getRowDataCache());
                }
                else {
                    columnReader = createColumnReader(
                            orcTypes.get(column.getColumnId()),
                            column,
                            Optional.ofNullable(filters.get(i)),
                            outputRequired ? Optional.of(includedColumns.get(i)) : Optional.empty(),
                            hiveStorageTimeZone,
                            systemMemoryContext);
                    if (orcCacheProperties.isRowDataCacheEnabled()) {
                        columnReader = SelectiveColumnReaders.wrapWithResultCachingStreamReader(columnReader, column,
                                predicate, orcCacheStore.getRowDataCache());
                    }
                }
                columnReaders[columnIndex] = columnReader;
                if (filters.get(i) != null) {
                    colReaderWithFilter.add(columnIndex);
                }
                else if (additionalFilters.get(i) != null && additionalFilters.get(i).size() > 0) {
                    colReaderWithORFilter.add(columnIndex);
                }
                else {
                    colReaderWithoutFilter.add(columnIndex);
                }
            }
        }

        return columnReaders;
    }
}
