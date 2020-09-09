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

package io.hetu.core.plugin.carbondata;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.BaseStorageFormat;
import io.prestosql.plugin.hive.HiveTableProperties;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.session.PropertyMetadata.enumProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static java.util.Locale.ENGLISH;

public class CarbondataTableProperties
        extends HiveTableProperties
{
    public static final String LOCATION_PROPERTY = "location";
    public static final String STORAGE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String SORTED_BY_PROPERTY = "sorted_by";
    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public CarbondataTableProperties(TypeManager typeManager, CarbondataConfig config)
    {
        super(typeManager, config);
        tableProperties = ImmutableList.of(
                stringProperty(
                        LOCATION_PROPERTY,
                        "File system location URI for the table",
                        null,
                        false),
                enumProperty(
                        STORAGE_FORMAT_PROPERTY,
                        "Hive storage format for the table",
                        CarbondataStorageFormat.class,
                        CarbondataStorageFormat.CARBON,
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<String> getCarbondataLocation(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(LOCATION_PROPERTY));
    }

    public static BaseStorageFormat getCarbondataStorageFormat(Map<String, Object> tableProperties)
    {
        return (BaseStorageFormat) tableProperties.get(STORAGE_FORMAT_PROPERTY);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitionedBy(Map<String, Object> tableProperties)
    {
        List<String> partitionedBy = (List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY);
        return partitionedBy == null ? ImmutableList.of() : ImmutableList.copyOf(partitionedBy);
    }

    @SuppressWarnings("unchecked")
    protected static List<SortingColumn> getSortedBy(Map<String, Object> tableProperties)
    {
        List<SortingColumn> sortedBy = (List<SortingColumn>) tableProperties.get(SORTED_BY_PROPERTY);
        return sortedBy == null ? ImmutableList.of() : ImmutableList.copyOf(sortedBy);
    }

    private static SortingColumn sortingColumnFromString(String name)
    {
        SortingColumn.Order order = SortingColumn.Order.ASCENDING;
        String lower = name.toUpperCase(ENGLISH);
        if (lower.endsWith(" ASC")) {
            name = name.substring(0, name.length() - 4).trim();
        }
        else if (lower.endsWith(" DESC")) {
            name = name.substring(0, name.length() - 5).trim();
            order = SortingColumn.Order.DESCENDING;
        }
        return new SortingColumn(name, order);
    }

    private static String sortingColumnToString(SortingColumn column)
    {
        return column.getColumnName() + ((column.getOrder() == SortingColumn.Order.DESCENDING) ? " DESC" : "");
    }
}
