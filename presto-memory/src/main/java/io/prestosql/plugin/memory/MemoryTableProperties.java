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
package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.util.Locale.ENGLISH;

public class MemoryTableProperties
{
    public static final String SPILL_COMPRESSION_PROPERTY = "spill_compression";
    public static final boolean SPILL_COMPRESSION_DEFAULT_VALUE = true;
    public static final String ASYNC_PROCESSING = "async_processing";
    public static final boolean ASYNC_PROCESSING_DEFAULT_VALUE = true;
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String SORTED_BY_PROPERTY = "sorted_by";
    public static final String INDEX_COLUMNS_PROPERTY = "index_columns";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public MemoryTableProperties(TypeManager typeManager, MemoryConfig config)
    {
        tableProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        INDEX_COLUMNS_PROPERTY,
                        "Index columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(Collectors.toList())),
                        value -> value),
                new PropertyMetadata<>(
                        PARTITIONED_BY_PROPERTY,
                        "Partition columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(Collectors.toList())),
                        value -> value),
                new PropertyMetadata<>(
                        SORTED_BY_PROPERTY,
                        "LogiPart sorting columns (sorting columns are automatically indexed)",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<?>) value).stream()
                                .map(String.class::cast)
                                .map(name -> name.toLowerCase(ENGLISH))
                                .map(MemoryTableProperties::sortingColumnFromString)
                                .collect(toImmutableList()),
                        value -> ((Collection<?>) value).stream()
                                .map(SortingColumn.class::cast)
                                .map(MemoryTableProperties::sortingColumnToString)
                                .collect(toImmutableList())),
                PropertyMetadata.booleanProperty(
                        SPILL_COMPRESSION_PROPERTY,
                        "Whether to enable page compression during spilling",
                        SPILL_COMPRESSION_DEFAULT_VALUE,
                        false),
                PropertyMetadata.booleanProperty(
                        ASYNC_PROCESSING,
                        "Whether to process LogicalParts asynchronously",
                        ASYNC_PROCESSING_DEFAULT_VALUE,
                        false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @SuppressWarnings("unchecked")
    public static List<String> getIndexedColumns(Map<String, Object> tableProperties)
    {
        List<String> indexedColumns = (List<String>) tableProperties.get(INDEX_COLUMNS_PROPERTY);
        return indexedColumns == null ? ImmutableList.of() : ImmutableList.copyOf(indexedColumns);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitionedBy(Map<String, Object> tableProperties)
    {
        List<String> partitionedBy = (List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY);
        return partitionedBy == null ? ImmutableList.of() : ImmutableList.copyOf(partitionedBy);
    }

    @SuppressWarnings("unchecked")
    public static List<SortingColumn> getSortedBy(Map<String, Object> tableProperties)
    {
        return (List<SortingColumn>) tableProperties.get(SORTED_BY_PROPERTY);
    }

    public static boolean getSpillCompressionEnabled(Map<String, Object> tableProperties)
    {
        Boolean spillCompressionEnabled = (Boolean) tableProperties.get(SPILL_COMPRESSION_PROPERTY);
        return spillCompressionEnabled == null ? SPILL_COMPRESSION_DEFAULT_VALUE : spillCompressionEnabled;
    }

    public static boolean getAsyncProcessingEnabled(Map<String, Object> tableProperties)
    {
        Boolean asyncProcessingEnabled = (Boolean) tableProperties.get(ASYNC_PROCESSING);
        return asyncProcessingEnabled == null ? ASYNC_PROCESSING_DEFAULT_VALUE : asyncProcessingEnabled;
    }

    private static SortingColumn sortingColumnFromString(String name)
    {
        SortOrder order = SortOrder.ASC_NULLS_LAST;
        String lower = name.toUpperCase(ENGLISH);
        if (lower.endsWith(" ASC")) {
            name = name.substring(0, name.length() - 4).trim();
        }
        else if (lower.endsWith(" DESC")) {
            name = name.substring(0, name.length() - 5).trim();
            order = SortOrder.DESC_NULLS_LAST;

            // TODO: support DESC
            throw new UnsupportedOperationException("DESC sort is not supported yet.");
        }
        return new SortingColumn(name, order);
    }

    private static String sortingColumnToString(SortingColumn column)
    {
        return column.getColumnName() + ((column.getOrder() == SortOrder.DESC_NULLS_FIRST) ? " DESC" : "");
    }
}
