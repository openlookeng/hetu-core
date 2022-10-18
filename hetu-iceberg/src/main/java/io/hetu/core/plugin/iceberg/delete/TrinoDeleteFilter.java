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
package io.hetu.core.plugin.iceberg.delete;

import io.hetu.core.plugin.iceberg.IcebergColumnHandle;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.hetu.core.plugin.iceberg.IcebergColumnHandle.TRINO_UPDATE_ROW_ID_COLUMN_ID;
import static io.hetu.core.plugin.iceberg.IcebergColumnHandle.TRINO_UPDATE_ROW_ID_COLUMN_NAME;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.FILE_PATH;
import static org.apache.iceberg.MetadataColumns.IS_DELETED;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;

public class TrinoDeleteFilter
        extends DeleteFilter<TrinoRow>
{
    private final FileIO fileIO;

    public TrinoDeleteFilter(FileScanTask task, Schema tableSchema, List<IcebergColumnHandle> requestedColumns, FileIO fileIO)
    {
        super(task, tableSchema, toSchema(tableSchema, requestedColumns));
        this.fileIO = requireNonNull(fileIO, "fileIO is null");
    }

    @Override
    protected StructLike asStructLike(TrinoRow row)
    {
        return row;
    }

    private static Schema toSchema(Schema tableSchema, List<IcebergColumnHandle> requestedColumns)
    {
        return new Schema(requestedColumns.stream().map(column -> toNestedField(tableSchema, column)).filter(a -> a != null).collect(toImmutableList()));
    }

    @Override
    protected InputFile getInputFile(String s)
    {
        return fileIO.newInputFile(s);
    }

    private static Schema filterSchema(Schema tableSchema, List<IcebergColumnHandle> requestedColumns)
    {
        Set<Integer> requestedFieldIds = requestedColumns.stream()
                .map(IcebergColumnHandle::getId)
                .collect(toImmutableSet());
        return new Schema(filterFieldList(tableSchema.columns(), requestedFieldIds));
    }

    private static List<Types.NestedField> filterFieldList(List<Types.NestedField> fields, Set<Integer> requestedFieldIds)
    {
        List<Types.NestedField> nestedFieldList = fields.stream()
                .map(field -> filterField(field, requestedFieldIds))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
        return nestedFieldList;
    }

    private static Optional<Types.NestedField> filterField(Types.NestedField field, Set<Integer> requestedFieldIds)
    {
        Type fieldType = field.type();
        if (requestedFieldIds.contains(field.fieldId())) {
            return Optional.of(field);
        }

        if (fieldType.isStructType()) {
            List<Types.NestedField> requiredChildren = filterFieldList(fieldType.asStructType().fields(), requestedFieldIds);
            if (requiredChildren.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(Types.NestedField.of(
                    field.fieldId(),
                    field.isOptional(),
                    field.name(),
                    Types.StructType.of(requiredChildren),
                    field.doc()));
        }

        return Optional.empty();
    }

    private static Types.NestedField toNestedField(Schema tableSchema, IcebergColumnHandle columnHandle)
    {
        if (columnHandle.isRowPositionColumn()) {
            return ROW_POSITION;
        }
        if (columnHandle.isIsDeletedColumn()) {
            return IS_DELETED;
        }
        if (columnHandle.isPathColumn()) {
            return FILE_PATH;
        }
        if (columnHandle.isUpdateRowIdColumn()) {
            return Types.NestedField.of(TRINO_UPDATE_ROW_ID_COLUMN_ID, false, TRINO_UPDATE_ROW_ID_COLUMN_NAME, Types.StructType.of());
        }

        Types.NestedField nestedField = tableSchema.findField(columnHandle.getId());

        return nestedField;
    }
}
