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
import io.hetu.core.spi.heuristicindex.SplitIndexMetadata;
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
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexMetadata;
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
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.prestosql.orc.reader.ColumnReaders.createColumnReader;
import static java.lang.Math.toIntExact;

public class OrcRecordReader
        extends AbstractOrcRecordReader<ColumnReader>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcRecordReader.class).instanceSize();
    private static final Logger log = Logger.get(OrcRecordReader.class);

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
            Map<String, Domain> domains,
            OrcCacheStore orcCacheStore,
            OrcCacheProperties orcCacheProperties)
            throws OrcCorruptionException
    {
        requireNonNull(readColumns, "readColumns is null");
        checkArgument(readColumns.stream().distinct().count() == readColumns.size(), "readColumns contains duplicate entries");
        requireNonNull(readTypes, "readTypes is null");
        checkArgument(readColumns.size() == readTypes.size(), "readColumns and readTypes must have the same size");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(fileStripes, "fileStripes is null");
        requireNonNull(stripeStats, "stripeStats is null");
        requireNonNull(orcDataSource, "orcDataSource is null");
        requireNonNull(orcTypes, "types is null");
        requireNonNull(decompressor, "decompressor is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(userMetadata, "userMetadata is null");
        requireNonNull(systemMemoryUsage, "systemMemoryUsage is null");
        requireNonNull(exceptionTransform, "exceptionTransform is null");

        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");
        this.writeChecksumBuilder = writeValidation.map(validation -> createWriteChecksumBuilder(orcTypes, readTypes));
        this.rowGroupStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(orcTypes, readTypes));
        this.stripeStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(orcTypes, readTypes));
        this.fileStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(orcTypes, readTypes));
        this.systemMemoryUsage = systemMemoryUsage.newAggregatedMemoryContext();
        this.blockFactory = new OrcBlockFactory(exceptionTransform, true);

        this.maxBlockBytes = requireNonNull(maxBlockSize, "maxBlockSize is null").toBytes();

        // it is possible that old versions of orc use 0 to mean there are no row groups
        checkArgument(rowsInRowGroup > 0, "rowsInRowGroup must be greater than zero");

        // sort stripes by file position
        List<StripeInfo> stripeInfos = new ArrayList<>();
        for (int i = 0; i < fileStripes.size(); i++) {
            Optional<StripeStatistics> stats = Optional.empty();
            // ignore all stripe stats if too few or too many
            if (stripeStats.size() == fileStripes.size()) {
                stats = stripeStats.get(i);
            }
            stripeInfos.add(new StripeInfo(fileStripes.get(i), stats));
        }
        stripeInfos.sort(comparingLong(info -> info.getStripe().getOffset()));

        // assumptions made about the index:
        // 1. they are all bitmap indexes
        // 2. the index split offset corresponds to the stripe offset

        // each stripe could have an index for multiple columns
        Map<Long, List<IndexMetadata>> stripeOffsetToIndex = new HashMap<>();
        if (indexes.isPresent() && !indexes.get().isEmpty()
                // check there is only one type of index
                && indexes.get().stream().map(i -> i.getIndex().getId()).collect(Collectors.toSet()).size() == 1) {
            for (IndexMetadata i : indexes.get()) {
                long offset = i.getSplitStart();

                stripeOffsetToIndex.putIfAbsent(offset, new LinkedList<>());

                List<IndexMetadata> stripeIndexes = stripeOffsetToIndex.get(offset);
                stripeIndexes.add(i);
            }
        }

        long totalRowCount = 0;
        long fileRowCount = 0;
        ImmutableList.Builder<StripeInformation> stripes = ImmutableList.builder();
        Map<StripeInformation, List<IndexMetadata>> stripeIndexes = new HashMap<>();
        ImmutableList.Builder<Long> stripeFilePositions = ImmutableList.builder();
        if (!fileStats.isPresent() || predicate.matches(numberOfRows, fileStats.get())) {
            // select stripes that start within the specified split
            for (int i = 0; i < stripeInfos.size(); i++) {
                StripeInfo info = stripeInfos.get(i);
                StripeInformation stripe = info.getStripe();
                if (splitContainsStripe(splitOffset, splitLength, stripe) && isStripeIncluded(stripe, info.getStats(), predicate)) {
                    stripes.add(stripe);
                    stripeFilePositions.add(fileRowCount);
                    totalRowCount += stripe.getNumberOfRows();

                    if (!stripeOffsetToIndex.isEmpty()) {
                        stripeIndexes.put(stripe, stripeOffsetToIndex.get(stripe.getOffset()));
                    }
                }
                fileRowCount += stripe.getNumberOfRows();
            }
        }
        this.totalRowCount = totalRowCount;
        this.stripes = stripes.build();
        this.stripeFilePositions = stripeFilePositions.build();

        // now that we know which stripes will be read, apply indexes on them if applicable
        // i.e. if an index exists for the pushed down predicates
        // once the indexes are applied, for each stripe we will have the rows inside
        // the stripe that matched the predicates
        stripeIndexes.entrySet().stream().forEach(stripeIndex -> {
            Map<Index, Domain> indexDomainMap = new HashMap<>();

            for (Map.Entry<String, Domain> domainEntry : domains.entrySet()) {
                String columnName = domainEntry.getKey();
                Domain columnDomain = domainEntry.getValue();

                // if the index exists, there should only be one index for this column within this stripe
                List<IndexMetadata> indexMetadata = stripeIndex.getValue().stream().filter(p -> p.getColumn().equalsIgnoreCase(columnName)).collect(Collectors.toList());
                if (indexMetadata.isEmpty() || indexMetadata.size() > 1) {
                    continue;
                }

                Index index = indexMetadata.get(0).getIndex();
                indexDomainMap.put(index, columnDomain);
            }

            if (!indexDomainMap.isEmpty()) {
                Iterator<Integer> thisStripeMatchingRows = indexDomainMap.entrySet().iterator().next().getKey().getMatches(indexDomainMap);

                if (thisStripeMatchingRows != null) {
                    PeekingIterator<Integer> peekingIterator = Iterators.peekingIterator(thisStripeMatchingRows);
                    stripeMatchingRows.put(stripeIndex.getKey(), peekingIterator);
                }
            }
        });

        orcDataSource = wrapWithCacheIfTinyStripes(orcDataSource, this.stripes, maxMergeDistance, tinyStripeThreshold);
        this.orcDataSource = orcDataSource;
        this.splitLength = splitLength;

        this.fileRowCount = stripeInfos.stream()
                .map(StripeInfo::getStripe)
                .mapToLong(StripeInformation::getNumberOfRows)
                .sum();

        this.userMetadata = ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slices::copyOf));

        this.currentStripeSystemMemoryContext = this.systemMemoryUsage.newAggregatedMemoryContext();
        // The streamReadersSystemMemoryContext covers the StreamReader local buffer sizes, plus leaf node StreamReaders'
        // instance sizes who use local buffers. SliceDirectStreamReader's instance size is not counted, because it
        // doesn't have a local buffer. All non-leaf level StreamReaders' (e.g. MapStreamReader, LongStreamReader,
        // ListStreamReader and StructStreamReader) instance sizes were not counted, because calling setBytes() in
        // their constructors is confusing.
        AggregatedMemoryContext streamReadersSystemMemoryContext = this.systemMemoryUsage.newAggregatedMemoryContext();

        stripeReader = new StripeReader(
/*=======
        super(readColumns,
                readTypes,
                predicate,
                numberOfRows,
                fileStripes,
                fileStats,
                stripeStats,
>>>>>>> Simple(deterministic) filter pushdown for HIVE_ORC(boolean, long, short, int, date, binary, string, char, varchar, short decimal) */
	/* Fixme(Nitin) : conflict resolve */
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
                orcCacheProperties);

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
        return new Page(batchSize, blocks);
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

    public ColumnReader[] createColumnReaders(
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
