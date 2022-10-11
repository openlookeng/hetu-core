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
package io.hetu.core.plugin.iceberg;

import io.prestosql.spi.type.Type;
import org.apache.iceberg.MetadataColumns;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.hetu.core.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public enum IcebergMetadataColumn
{
    FILE_PATH(MetadataColumns.FILE_PATH.fieldId(), "$path", VARCHAR, PRIMITIVE),
    /**/;

    private static final Set<Integer> COLUMNS_ID = Stream.of(values())
            .map(IcebergMetadataColumn::getId)
            .collect(toImmutableSet());
    private final int id;
    private final String columnName;
    private final Type type;
    private final ColumnIdentity.TypeCategory typeCategory;

    IcebergMetadataColumn(int id, String columnName, Type type, ColumnIdentity.TypeCategory typeCategory)
    {
        this.id = id;
        this.columnName = columnName;
        this.type = type;
        this.typeCategory = typeCategory;
    }

    public int getId()
    {
        return id;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public Type getType()
    {
        return type;
    }

    public ColumnIdentity.TypeCategory getTypeCategory()
    {
        return typeCategory;
    }

    public static boolean isMetadataColumnId(int id)
    {
        return COLUMNS_ID.contains(id);
    }
}
