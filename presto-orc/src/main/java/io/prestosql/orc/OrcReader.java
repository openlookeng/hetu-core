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
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.ExceptionWrappingMetadataReader;
import io.prestosql.orc.metadata.Footer;
import io.prestosql.orc.metadata.Metadata;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.OrcMetadataReader;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.metadata.OrcType.OrcTypeKind;
import io.prestosql.orc.metadata.PostScript.HiveWriterVersion;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.SplitMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.metadata.OrcColumnId.ROOT_COLUMN;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class OrcReader
{
    public static final int MAX_BATCH_SIZE = 1024;
    public static final int INITIAL_BATCH_SIZE = 1;
    public static final int BATCH_SIZE_GROWTH_FACTOR = 2;

    private static final Logger log = Logger.get(OrcReader.class);

    private final OrcDataSource orcDataSource;
    private final ExceptionWrappingMetadataReader metadataReader;
    private final DataSize maxMergeDistance;
    private final DataSize tinyStripeThreshold;
    private final DataSize maxBlockSize;
    private final HiveWriterVersion hiveWriterVersion;
    private final int bufferSize;
    private final CompressionKind compressionKind;
    private final Optional<OrcDecompressor> decompressor;
    private final Footer footer;
    private final Metadata metadata;
    private final OrcColumn rootColumn;
    private final Optional<OrcWriteValidation> writeValidation;

    // This is based on the Apache Hive ORC code
    public OrcReader(OrcDataSource orcDataSource, DataSize maxMergeDistance, DataSize tinyStripeThreshold, DataSize maxBlockSize)
            throws IOException
    {
        this(orcDataSource, maxMergeDistance, tinyStripeThreshold, maxBlockSize, Optional.empty());
    }

    private OrcReader(
            OrcDataSource orcDataSource,
            DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            Optional<OrcWriteValidation> writeValidation)
            throws IOException
    {
        this.orcDataSource = wrapWithCacheIfTiny(orcDataSource, tinyStripeThreshold);
        OrcFileTail fileTail = OrcFileTail.readFrom(this.orcDataSource, writeValidation);
        this.metadataReader = new ExceptionWrappingMetadataReader(orcDataSource.getId(), new OrcMetadataReader());
        this.bufferSize = toIntExact(fileTail.getPostScript().getCompressionBlockSize());
        this.compressionKind = fileTail.getPostScript().getCompression();
        this.hiveWriterVersion = fileTail.getPostScript().getHiveWriterVersion();
        this.decompressor = fileTail.getDecompressor();
        this.metadata = fileTail.getMetadata();
        this.footer = fileTail.getFooter();
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.tinyStripeThreshold = requireNonNull(tinyStripeThreshold, "tinyStripeThreshold is null");
        this.maxBlockSize = requireNonNull(maxBlockSize, "maxBlockSize is null");
        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");
        this.rootColumn = createOrcColumn("", "", new OrcColumnId(0), footer.getTypes(), orcDataSource.getId());
    }

    public OrcReader(OrcDataSource orcDataSource, OrcFileTail fileTail, DataSize maxMergeDistance,
                     DataSize tinyStripeThreshold, DataSize maxBlockSize)
    {
        this.orcDataSource = orcDataSource;
        this.metadataReader = new ExceptionWrappingMetadataReader(orcDataSource.getId(), new OrcMetadataReader());
        this.bufferSize = toIntExact(fileTail.getPostScript().getCompressionBlockSize());
        this.compressionKind = fileTail.getPostScript().getCompression();
        this.hiveWriterVersion = fileTail.getPostScript().getHiveWriterVersion();
        this.decompressor = fileTail.getDecompressor();
        this.metadata = fileTail.getMetadata();
        this.footer = fileTail.getFooter();
        this.rootColumn = createOrcColumn("", "", new OrcColumnId(0), footer.getTypes(), orcDataSource.getId());
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.tinyStripeThreshold = requireNonNull(tinyStripeThreshold, "tinyStripeThreshold is null");
        this.maxBlockSize = requireNonNull(maxBlockSize, "maxBlockSize is null");
        this.writeValidation = Optional.empty();
    }

    public List<String> getColumnNames()
    {
        return footer.getTypes().get(ROOT_COLUMN).getFieldNames();
    }

    public Footer getFooter()
    {
        return footer;
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public OrcColumn getRootColumn()
    {
        return rootColumn;
    }

    public int getBufferSize()
    {
        return bufferSize;
    }

    public CompressionKind getCompressionKind()
    {
        return compressionKind;
    }

    public OrcRecordReader createRecordReader(
            List<OrcColumn> readColumns,
            List<Type> readTypes,
            OrcPredicate predicate,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryUsage,
            int initialBatchSize,
            Function<Exception, RuntimeException> exceptionTransform)
            throws OrcCorruptionException
    {
        return createRecordReader(
                readColumns,
                readTypes,
                predicate,
                0,
                orcDataSource.getSize(),
                hiveStorageTimeZone,
                systemMemoryUsage,
                initialBatchSize,
                exceptionTransform,
                Optional.empty(),
                null,
                Collections.emptyMap(),
                OrcCacheStore.CACHE_NOTHING,
                new OrcCacheProperties(),
                false);
    }

    public OrcRecordReader createRecordReader(
            List<OrcColumn> readColumns,
            List<Type> readTypes,
            OrcPredicate predicate,
            long offset,
            long length,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryUsage,
            int initialBatchSize,
            Function<Exception, RuntimeException> exceptionTransform)
            throws OrcCorruptionException
    {
        return new OrcRecordReader(
                requireNonNull(readColumns, "readColumns is null"),
                requireNonNull(readTypes, "readTypes is null"),
                requireNonNull(predicate, "predicate is null"),
                footer.getNumberOfRows(),
                footer.getStripes(),
                footer.getFileStats(),
                metadata.getStripeStatsList(),
                orcDataSource,
                offset,
                length,
                footer.getTypes(),
                decompressor,
                footer.getRowsInRowGroup(),
                requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null"),
                hiveWriterVersion,
                metadataReader,
                maxMergeDistance,
                tinyStripeThreshold,
                maxBlockSize,
                footer.getUserMetadata(),
                systemMemoryUsage,
                writeValidation,
                initialBatchSize,
                exceptionTransform,
                Optional.empty(),
                null,
                Collections.emptyMap(),
                OrcCacheStore.CACHE_NOTHING,
                new OrcCacheProperties(),
                false);
    }

    public OrcRecordReader createRecordReader(
            List<OrcColumn> readColumns,
            List<Type> readTypes,
            OrcPredicate predicate,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryUsage,
            int initialBatchSize,
            Function<Exception, RuntimeException> exceptionTransform,
            OrcCacheStore orcCacheStore,
            OrcCacheProperties orcCacheProperties)
            throws OrcCorruptionException
    {
        return createRecordReader(
                readColumns,
                readTypes,
                predicate,
                0,
                orcDataSource.getSize(),
                hiveStorageTimeZone,
                systemMemoryUsage,
                initialBatchSize,
                exceptionTransform,
                Optional.empty(),
                null,
                Collections.emptyMap(),
                orcCacheStore,
                orcCacheProperties,
                false);
    }

    public OrcRecordReader createRecordReader(
            List<OrcColumn> readColumns,
            List<Type> readTypes,
            OrcPredicate predicate,
            long offset,
            long length,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryUsage,
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
        return new OrcRecordReader(
                requireNonNull(readColumns, "readColumns is null"),
                requireNonNull(readTypes, "readTypes is null"),
                requireNonNull(predicate, "predicate is null"),
                footer.getNumberOfRows(),
                footer.getStripes(),
                footer.getFileStats(),
                metadata.getStripeStatsList(),
                orcDataSource,
                offset,
                length,
                footer.getTypes(),
                decompressor,
                footer.getRowsInRowGroup(),
                requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null"),
                hiveWriterVersion,
                metadataReader,
                maxMergeDistance,
                tinyStripeThreshold,
                maxBlockSize,
                footer.getUserMetadata(),
                systemMemoryUsage,
                writeValidation,
                initialBatchSize,
                exceptionTransform,
                indexes,
                splitMetadata,
                domains,
                orcCacheStore,
                orcCacheProperties,
                pageMetadataEnabled);
    }

    public OrcSelectiveRecordReader createSelectiveRecordReader(
            List<OrcColumn> fileColumns,
            List<OrcColumn> fileReadColumns,
            List<Type> readTypes,
            List<Integer> outputColumns,
            Map<Integer, Type> includedColumns,
            Map<Integer, TupleDomainFilter> filters,
            Map<Integer, Object> constantValues,
            OrcPredicate predicate,
            long offset,
            long length,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryUsage,
            int initialBatchSize,
            Function<Exception, RuntimeException> exceptionTransform,
            Optional<List<IndexMetadata>> indexes,
            Map<String, Domain> domains,
            OrcCacheStore orcCacheStore,
            OrcCacheProperties orcCacheProperties,
            Optional<OrcWriteValidation> writeValidation,
            Map<Integer, List<TupleDomainFilter>> disjunctFilters,
            List<Integer> positions, boolean useDataCache,
            Map<Integer, Function<Block, Block>> coercer,
            Map<String, List<Domain>> orDomains,
            Set<Integer> missingColumns) throws OrcCorruptionException
    {
        return new OrcSelectiveRecordReader(
                outputColumns,
                includedColumns,
                requireNonNull(fileColumns, "readColumns is null"),
                requireNonNull(fileReadColumns, "readColumns is null"),
                requireNonNull(readTypes, "readTypes is null"),
                filters,
                constantValues,
                requireNonNull(predicate, "predicate is null"),
                footer.getNumberOfRows(),
                footer.getStripes(),
                footer.getFileStats(),
                metadata.getStripeStatsList(),
                orcDataSource,
                offset,
                length,
                footer.getTypes(),
                decompressor,
                footer.getRowsInRowGroup(),
                requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null"),
                hiveWriterVersion,
                metadataReader,
                maxMergeDistance,
                tinyStripeThreshold,
                maxBlockSize,
                footer.getUserMetadata(),
                systemMemoryUsage,
                writeValidation,
                initialBatchSize,
                exceptionTransform,
                indexes,
                domains,
                orcCacheStore,
                orcCacheProperties,
                disjunctFilters,
                positions,
                useDataCache,
                coercer,
                orDomains,
                missingColumns);
    }

    public static OrcDataSource wrapWithCacheIfTiny(OrcDataSource dataSource, DataSize maxCacheSize)
    {
        if (dataSource instanceof CachingOrcDataSource) {
            return dataSource;
        }
        if (dataSource.getSize() > maxCacheSize.toBytes()) {
            return dataSource;
        }
        DiskRange diskRange = new DiskRange(0, toIntExact(dataSource.getSize()));
        return new CachingOrcDataSource(dataSource, desiredOffset -> diskRange);
    }

    private static OrcColumn createOrcColumn(
            String parentStreamName,
            String fieldName,
            OrcColumnId columnId,
            ColumnMetadata<OrcType> types,
            OrcDataSourceId orcDataSourceId)
    {
        String path = fieldName.isEmpty() ? parentStreamName : parentStreamName + "." + fieldName;
        OrcType orcType = types.get(columnId);

        List<OrcColumn> nestedColumns = ImmutableList.of();
        if (orcType.getOrcTypeKind() == OrcTypeKind.STRUCT) {
            nestedColumns = IntStream.range(0, orcType.getFieldCount())
                    .mapToObj(fieldId -> createOrcColumn(
                            path,
                            orcType.getFieldName(fieldId),
                            orcType.getFieldTypeIndex(fieldId),
                            types,
                            orcDataSourceId))
                    .collect(toImmutableList());
        }
        else if (orcType.getOrcTypeKind() == OrcTypeKind.LIST) {
            nestedColumns = ImmutableList.of(createOrcColumn(path, "item", orcType.getFieldTypeIndex(0), types, orcDataSourceId));
        }
        else if (orcType.getOrcTypeKind() == OrcTypeKind.MAP) {
            nestedColumns = ImmutableList.of(
                    createOrcColumn(path, "key", orcType.getFieldTypeIndex(0), types, orcDataSourceId),
                    createOrcColumn(path, "value", orcType.getFieldTypeIndex(1), types, orcDataSourceId));
        }
        return new OrcColumn(path, columnId, fieldName, orcType.getOrcTypeKind(), orcDataSourceId, nestedColumns);
    }

    static void validateFile(
            OrcWriteValidation writeValidation,
            OrcDataSource input,
            List<Type> readTypes,
            DateTimeZone hiveStorageTimeZone)
            throws OrcCorruptionException
    {
        try {
            OrcReader orcReader = new OrcReader(input, new DataSize(1, MEGABYTE), new DataSize(8, MEGABYTE), new DataSize(16, MEGABYTE), Optional.of(writeValidation));
            try (OrcRecordReader orcRecordReader = orcReader.createRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    readTypes,
                    OrcPredicate.TRUE,
                    hiveStorageTimeZone,
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE,
                    exception -> {
                        throwIfUnchecked(exception);
                        return new RuntimeException(exception);
                    })) {
                for (Page page = orcRecordReader.nextPage(); page != null; page = orcRecordReader.nextPage()) {
                    // fully load the page
                    page.getLoadedPage();
                }
            }
        }
        catch (IOException e) {
            throw new OrcCorruptionException(e, input.getId(), "Validation failed");
        }
    }

    public static void handleCacheLoadException(Exception executionException)
            throws IOException
    {
        if (Thread.currentThread().isInterrupted()) {
            if (executionException.getCause() instanceof PrestoException) {
                throw (PrestoException) executionException.getCause();
            }
            throw new IOException(executionException.getCause());
        }
    }
}
