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
import io.prestosql.spi.type.TypeNotFoundException;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import org.joda.time.DateTimeZone;

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
    private static final Logger log = Logger.get(OrcSelectiveRecordReader.class);
    private static final byte[] NULL_MARKER = new byte[0];

    private final List<Integer> excludePositions;
    private final List<Integer> columnReaderOrder;
    private final Map<Integer, TupleDomainFilter> filters;
    List<Integer> outputColumns;
    Map<Integer, Type> includedColumns;

    private final Map<Integer, Object> constantValues;

    Set<Integer> colReaderWithFilter;
    Set<Integer> colReaderWithORFilter;
    Set<Integer> colReaderWithoutFilter;
    Map<Integer, List<TupleDomainFilter>> disjuctFilters;
    Map<Integer, Function<Block, Block>> coercers;
    private final Set<Integer> missingColumns;
    // flag indicating whether range filter on a constant column is false; no data is read in that case
    private boolean constantFilterIsFalse;

    /**
     * Create a selective record reader to be used with selective page source.
     * This reader is different from main reader in terms:
     * 1. Applied filter on each column during read of each column value.
     * 2. Matching row position from current column read is passed as input to subsequent column read, so next column
     *    needs to read only partial data.
     * 3. If a particular column is involved in only filter (but no projection), then its values not saved.
     * 4. Finally a Page block is formed only on final matching rows.
     *
     * For more details how read logic works, check SelectiveColumnReader.read.
     * @param outputColumns List of columns which needs to be projected.
     * @param includedColumns List of columns which are part of scan and all of these needs to be read so we need
     *                        reader for all these columns.
     * @param filters   Map of each column index with the corresponding filter.
     * @param constantValues Constant value corresponding to all prefilled columns (e.g. partition key).
     * @param predicate This will be used for split filtering.
     * @param disjuctFilters Filters corresponding to OR clause.
     * @param useDataCache Enabled/disable use of data cache.
     * @param coercers Map of coercion function corresponding to column index.
     * @param missingColumns List of all columns which are not file but part of scan.
     * @throws OrcCorruptionException
     */
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
            Map<Integer, List<TupleDomainFilter>> disjuctFilters,
            List<Integer> positions,
            boolean useDataCache,
            Map<Integer, Function<Block, Block>> coercers,
            Map<String, List<Domain>> orDomains,
            Set<Integer> missingColumns)
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
                orDomains,
                false);

        int fieldCount = orcTypes.get(OrcColumnId.ROOT_COLUMN).getFieldCount();
        this.columnReaderOrder = new ArrayList<>(fieldCount);

        this.outputColumns = outputColumns;
        this.includedColumns = includedColumns;
        this.excludePositions = positions;

        this.filters = filters;
        this.disjuctFilters = disjuctFilters;
        this.constantValues = requireNonNull(constantValues, "constantValues is null");
        this.coercers = requireNonNull(coercers, "coercers is null");
        this.missingColumns = requireNonNull(missingColumns, "missingColumns is null");

        for (Map.Entry<Integer, Function<Block, Block>> entry : coercers.entrySet()) {
            checkArgument(!filters.containsKey(entry.getKey()), "Coercions for columns with range filters are not yet supported");
        }

        for (int missingColumn : missingColumns) {
            if (!constantFilterIsFalse && containsNonNullFilter(filters.get(missingColumn))) {
                constantFilterIsFalse = true;
            }

            this.constantValues.put(missingColumn, NULL_MARKER);
        }

        setColumnReadersParam(createColumnReaders(fileColumns,
                systemMemoryUsage.newAggregatedMemoryContext(),
                new OrcBlockFactory(exceptionTransform, true),
                orcCacheStore, orcCacheProperties,
                predicate, filters, hiveStorageTimeZone,
                outputColumns, includedColumns, orcTypes, useDataCache));
    }

    private static boolean containsNonNullFilter(TupleDomainFilter columnFilters)
    {
        return columnFilters != null && !columnFilters.testNull();
    }

    /* Perform ORC read by eliminating non-matching records before forming the data blocks
     * This is carried out in 3 stages;
     *  I)   Filter using Conjuncts (AND'd operators)
     *  II)  Filter using discjuncts (OR'd operators)
     *  III) Fields without filters
     *  Finally compose the block/page with the matching records.
     */
    public Page getNextPage()
            throws IOException
    {
        int batchSize = prepareNextBatch();
        if (batchSize < 0) {
            return null;
        }

        // if there is no OR filer and constantFilterIsFalse (i.e. one of the missing column contains NOT NULL filter)
        // is true means no record will qualify filter in current split.
        if (constantFilterIsFalse && colReaderWithORFilter.isEmpty()) {
            batchRead(batchSize);
            return new Page(0);
        }

        matchingRowsInBatchArray = null;
        int[] positionsToRead = initializePositions(batchSize);
        int positionCount = positionsToRead.length;

        /* first evaluate columns with filter AND conditions */
        SelectiveColumnReader[] columnReaders = getColumnReaders();
        if (positionCount != 0) {
            for (Integer columnIdx : colReaderWithFilter) {
                if (columnIdx < 0) {
                    if (!matchConstantWithPredicate(includedColumns.get(columnIdx), constantValues.get(columnIdx), filters.get(columnIdx))) {
                        positionCount = 0;
                        break;
                    }
                }
                else if (missingColumns.contains(columnIdx)) {
                    if (!filters.get(columnIdx).testNull()) {
                        positionCount = 0;
                        break;
                    }
                }
                else if (columnReaders[columnIdx] != null) {
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

        /* Perform OR filtering:
         *    OR filtering is applied with 2 level; identify match and exclude.
         *    -  Identify:  Read all the matching records using the available positions by apply the filter to each
         *                  row position from Stage-1; and accumulate position in the accumulator.
         *    - Exclude:    Exclude all records which did not matched in any pass of the filters applied; i.e.
         *                  accumulator set becomes the final matching row positions.
         */
        BitSet accumulator = new BitSet();
        if (colReaderWithORFilter.size() > 0 && positionCount > 0) {
            int localPositionCount = positionCount;
            for (Integer columnIdx : colReaderWithORFilter) {
                if (columnIdx < 0) {
                    if (matchConstantWithPredicate(includedColumns.get(columnIdx), constantValues.get(columnIdx), disjuctFilters.get(columnIdx).get(0))) {
                        /* Skip OR filtering all will match */
                        accumulator.set(positionsToRead[0], positionsToRead[positionCount - 1] + 1);
                    }
                }
                else if (missingColumns.contains(columnIdx)) {
                    if (disjuctFilters.get(columnIdx).get(0).testNull()) {
                        /* Skip OR filtering all will match */
                        accumulator.set(positionsToRead[0], positionsToRead[positionCount - 1] + 1);
                    }
                }
                else if (columnReaders[columnIdx] != null) {
                    localPositionCount += columnReaders[columnIdx].readOr(getNextRowInGroup(), positionsToRead, positionCount, disjuctFilters.get(columnIdx), accumulator);
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
            if (columnIndex < 0 || missingColumns.contains(columnIndex)) {
                // To fill partition key.
                blocks[i] = RunLengthEncodedBlock.create(includedColumns.get(columnIndex), constantValues.get(columnIndex) == NULL_MARKER ? null : constantValues.get(columnIndex), positionCount);
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

    private int[] initializePositions(int batchSize)
    {
        // currentPosition to currentBatchSize
        StripeInformation stripe = stripes.get(currentStripe);

        if (matchingRowsInBatchArray == null && stripeMatchingRows.containsKey(stripe)) {
            long currentPositionInStripe = currentPosition - currentStripePosition;

            PeekingIterator<Integer> matchingRows = stripeMatchingRows.get(stripe);
            List<Integer> matchingRowsInBlock = new ArrayList<>();

            while (matchingRows.hasNext()) {
                Integer row = matchingRows.peek();
                if (row < currentPositionInStripe) {
                    // this can happen if a row group containing matching rows was filtered out
                    // for example, if matchingRows is for column1 but query is for column1 and column2.
                    // since row groups have minmax values, a row group could have been filtered out because of
                    // column2 predicate. this means that the current matchingRow could be 10 (within the first
                    // row group), but the first row group might've been filtered out due to column2 predicate,
                    // so currentPositionInStripe is already in second row group
                    //
                    // stripe 1
                    //    -> row group 1 (rows 1 to 10000) [filtered out due to column2 predicate]
                    //       1
                    //       2
                    //       ...
                    //       10     <- matchingRows cursor is here, but this row group has been filtered out
                    //       ...
                    //       10000
                    //    -> row group 2 (rows 10001 to 20000)
                    //       10001
                    //       10002   <- currentPositionInStripe is here
                    //       ...
                    //       20000
                    matchingRows.next();
                }
                else if (row < currentPositionInStripe + batchSize) {
                    // matchingRows cursor is within current batch
                    matchingRowsInBlock.add(toIntExact(Long.valueOf(row) - currentPositionInStripe));
                    matchingRows.next();
                }
                else {
                    // matchingRows cursor is ahead of current batch, next batch will use it
                    break;
                }
            }

            matchingRowsInBatchArray = new int[matchingRowsInBlock.size()];
            IntStream.range(0, matchingRowsInBlock.size()).forEach(
                    i -> matchingRowsInBatchArray[i] = matchingRowsInBlock.get(i));
        }

        if (matchingRowsInBatchArray != null) {
            return matchingRowsInBatchArray;
        }
        else {
            int[] positions = new int[batchSize];
            for (int i = 0; i < batchSize; i++) {
                positions[i] = i;
            }
            return positions;
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
        IntArraySet remainingColumns = new IntArraySet();
        remainingColumns.addAll(includedColumns.keySet());

        for (int i = 0; i < fieldCount; i++) {
            // create column reader only for columns which are part of projection and filter.
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
                else if (disjuctFilters.get(i) != null && disjuctFilters.get(i).size() > 0) {
                    colReaderWithORFilter.add(columnIndex);
                }
                else {
                    colReaderWithoutFilter.add(columnIndex);
                }

                remainingColumns.remove(columnIndex);
            }
        }

        /* if any still remaining colIdx < 0 */
        remainingColumns.removeAll(missingColumns);
        for (Integer col : remainingColumns) {
            if (col < 0) { /* should be always true! */
                if (filters.get(col) != null) {
                    colReaderWithFilter.add(col);
                }
                else if (disjuctFilters.get(col) != null && disjuctFilters.get(col).size() > 0) {
                    colReaderWithORFilter.add(col);
                }
            }
        }

        // specially for alter add column case:
        for (int missingColumn : missingColumns) {
            if (filters.get(missingColumn) != null) {
                colReaderWithFilter.add(missingColumn);
            }
            else if (disjuctFilters.get(missingColumn) != null && disjuctFilters.get(missingColumn).size() > 0) {
                colReaderWithORFilter.add(missingColumn);
            }
        }

        return columnReaders;
    }

    private boolean matchConstantWithPredicate(Type type, Object value, TupleDomainFilter filter)
    {
        if (value == null) {
            return filter.testNull();
        }
        else if (type.getJavaType() == boolean.class) {
            return filter.testBoolean((Boolean) value);
        }
        else if (type.getJavaType() == double.class) {
            return filter.testDouble(((Number) value).doubleValue());
        }
        else if (type.getJavaType() == float.class) {
            return filter.testFloat(((Number) value).floatValue());
        }
        else if (type.getJavaType() == long.class) {
            return filter.testLong(((Number) value).longValue());
        }
        else if (type.getJavaType() == int.class) {
            return filter.testLong(((Number) value).intValue());
        }
        else if (type.getJavaType() == Slice.class) {
            byte[] strVal;
            if (value instanceof byte[]) {
                strVal = (byte[]) value;
            }
            else if (value instanceof String) {
                strVal = ((String) value).getBytes();
            }
            else {
                strVal = ((Slice) value).getBytes();
            }
            return filter.testBytes(strVal, 0, strVal.length);
        }
        else if (type.getJavaType() == long[].class) {
            long[] data = (long[]) value;
            return filter.testDecimal(data[0], data[1]);
        }

        throw new TypeNotFoundException(type.getTypeSignature());
    }
}
