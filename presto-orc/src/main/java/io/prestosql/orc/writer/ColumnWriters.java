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

package io.prestosql.orc.writer;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.metadata.statistics.BinaryStatisticsBuilder;
import io.prestosql.orc.metadata.statistics.BloomFilterBuilder;
import io.prestosql.orc.metadata.statistics.DateStatisticsBuilder;
import io.prestosql.orc.metadata.statistics.DoubleStatisticsBuilder;
import io.prestosql.orc.metadata.statistics.IntegerStatisticsBuilder;
import io.prestosql.orc.metadata.statistics.NoOpBloomFilterBuilder;
import io.prestosql.orc.metadata.statistics.StringStatisticsBuilder;
import io.prestosql.orc.metadata.statistics.TimestampStatisticsBuilder;
import io.prestosql.orc.metadata.statistics.Utf8BloomFilterBuilder;
import io.prestosql.spi.type.Type;

import java.util.function.Supplier;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class ColumnWriters
{
    private static final int BASE_OFFSET_FOR_TRANSACTIONAL_TABLE = 6;

    private ColumnWriters() {}

    public static ColumnWriter createColumnWriter(
            OrcColumnId columnId,
            ColumnMetadata<OrcType> orcTypes,
            Type type,
            CompressionKind compression,
            int bufferSize,
            OrcWriterOptions options,
            String columnName,
            Supplier<BloomFilterBuilder> bloomFilterBuilder)
    {
        requireNonNull(type, "type is null");
        OrcType orcType = orcTypes.get(columnId);
        DataSize stringStatisticsLimit = options.getMaxStringStatisticsLimit();

        switch (orcType.getOrcTypeKind()) {
            case BOOLEAN:
                return new BooleanColumnWriter(columnId, type, compression, bufferSize);

            case FLOAT:
                return new FloatColumnWriter(columnId, type, compression, bufferSize, () -> new DoubleStatisticsBuilder(bloomFilterBuilder.get()));

            case DOUBLE:
                return new DoubleColumnWriter(columnId, type, compression, bufferSize, () -> new DoubleStatisticsBuilder(bloomFilterBuilder.get()));

            case BYTE:
                return new ByteColumnWriter(columnId, type, compression, bufferSize);

            case DATE:
                return new LongColumnWriter(columnId, type, compression, bufferSize, () -> new DateStatisticsBuilder(bloomFilterBuilder.get()));

            case SHORT:
            case INT:
            case LONG:
                return new LongColumnWriter(columnId, type, compression, bufferSize, () -> new IntegerStatisticsBuilder(bloomFilterBuilder.get()));

            case DECIMAL:
                return new DecimalColumnWriter(columnId, type, compression, bufferSize);

            case TIMESTAMP:
                return new TimestampColumnWriter(columnId, type, compression, bufferSize, () -> new TimestampStatisticsBuilder(bloomFilterBuilder.get()));

            case BINARY:
                return new SliceDirectColumnWriter(columnId, type, compression, bufferSize, BinaryStatisticsBuilder::new);

            case CHAR:
            case VARCHAR:
            case STRING:
                return new SliceDictionaryColumnWriter(columnId, type, compression, bufferSize, () -> new StringStatisticsBuilder(toIntExact(stringStatisticsLimit.toBytes()), bloomFilterBuilder.get()));

            case LIST: {
                OrcColumnId fieldColumnIndex = orcType.getFieldTypeIndex(0);
                Type fieldType = type.getTypeParameters().get(0);
                ColumnWriter elementWriter = createColumnWriter(fieldColumnIndex, orcTypes, fieldType, compression, bufferSize, options, columnName, bloomFilterBuilder);
                return new ListColumnWriter(columnId, compression, bufferSize, elementWriter);
            }

            case MAP: {
                ColumnWriter keyWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(0),
                        orcTypes,
                        type.getTypeParameters().get(0),
                        compression,
                        bufferSize,
                        options,
                        columnName,
                        bloomFilterBuilder);
                ColumnWriter valueWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(1),
                        orcTypes,
                        type.getTypeParameters().get(1),
                        compression,
                        bufferSize,
                        options,
                        columnName,
                        bloomFilterBuilder);
                return new MapColumnWriter(columnId, compression, bufferSize, keyWriter, valueWriter);
            }

            case STRUCT: {
                ImmutableList.Builder<ColumnWriter> fieldWriters = ImmutableList.builder();
                for (int fieldId = 0; fieldId < orcType.getFieldCount(); fieldId++) {
                    OrcColumnId fieldColumnIndex = orcType.getFieldTypeIndex(fieldId);
                    Type fieldType = type.getTypeParameters().get(fieldId);
                    String colName = isTransactional(options) && columnId.getId() == BASE_OFFSET_FOR_TRANSACTIONAL_TABLE ? orcType.getFieldName(fieldId) : columnName + "." + orcType.getFieldName(fieldId);
                    fieldWriters.add(createColumnWriter(fieldColumnIndex,
                            orcTypes,
                            fieldType,
                            compression,
                            bufferSize,
                            options,
                            colName,
                            getBloomFilterBuilder(options, colName, fieldColumnIndex.getId())));
                }
                return new StructColumnWriter(columnId, compression, bufferSize, fieldWriters.build());
            }
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    public static Supplier<BloomFilterBuilder> getBloomFilterBuilder(OrcWriterOptions options, String columnName, Integer columnId)
    {
        if (columnId > options.getBaseIndex() && options.isBloomFilterColumn(columnName)) {
            return () -> new Utf8BloomFilterBuilder(options.getRowGroupMaxRowCount(), options.getBloomFilterFpp());
        }
        return NoOpBloomFilterBuilder::new;
    }

    public static boolean isTransactional(OrcWriterOptions options)
    {
        return options.getBaseIndex() == 0 ? false : true;
    }
}
