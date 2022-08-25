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
package io.hetu.core.plugin.iceberg.util;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.FullConnectorSession;
import io.prestosql.Session;
import io.prestosql.icebergutil.TestSchemaMetadata;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.orc.OrcWriteValidation;
import io.prestosql.plugin.hive.HiveCompressionCodec;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.hetu.core.plugin.iceberg.TypeConverter.toIcebergType;
import static io.prestosql.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.prestosql.plugin.hive.HiveCompressionCodec.ZSTD;
import static io.prestosql.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.prestosql.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;
import static io.prestosql.spi.session.PropertyMetadata.doubleProperty;
import static io.prestosql.spi.session.PropertyMetadata.enumProperty;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class IcebergTestUtil
{
    private IcebergTestUtil()
    {
    }

    public static HiveMetastore getHiveMetastore()
            throws IOException
    {
        File hiveDir = new File(createTempDirectory("TestTpcdsCostBase").toFile(), "test_schema");
        return createTestingFileHiveMetastore(hiveDir);
    }

    public static TestSchemaMetadata getMetadata()
    {
        return new TestSchemaMetadata();
    }

    public static ConnectorSession getConnectorSession(TestSchemaMetadata metadata)
    {
        TransactionManager transactionManager = metadata.transactionManager;
        TransactionId transactionId = transactionManager.beginTransaction(false);
        return metadata.createNewSession(transactionId);
    }

    public static ConnectorSession getConnectorSession2(TestSchemaMetadata metadata)
    {
        TransactionManager transactionManager = metadata.transactionManager;
        TransactionId transactionId = transactionManager.beginTransaction(false);
        return metadata.createNewSession2(transactionId);
    }

    public static ConnectorSession getConnectorSession3(Session session, SessionPropertyManager sessionPropertyManager, Map<CatalogName, Map<String, String>> map, TestSchemaMetadata metadata)
    {
        TransactionManager transactionManager = metadata.transactionManager;
        TransactionId transactionId = transactionManager.beginTransaction(false);
        return metadata.createNewSession3(session, sessionPropertyManager, map, transactionId);
    }

    public static ConnectorSession getConnectorSession()
    {
        TestSchemaMetadata metadata = new TestSchemaMetadata();
        TransactionManager transactionManager = metadata.transactionManager;
        TransactionId transactionId = transactionManager.beginTransaction(false);
        return metadata.createNewSession(transactionId);
    }

    public static Session getSession(TestSchemaMetadata metadata)
    {
        TransactionManager transactionManager = metadata.transactionManager;
        TransactionId transactionId = transactionManager.beginTransaction(false);
        FullConnectorSession newSession = (FullConnectorSession) metadata.createNewSession(transactionId);
        return newSession.getSession();
    }

    public static Session getSession()
    {
        FullConnectorSession fullConnectorSession = (FullConnectorSession) getConnectorSession();
        return fullConnectorSession.getSession();
    }

    public static Schema createSchema2(Map<String, Type> map)
    {
        List<Types.NestedField> objects = new ArrayList<>();
        int[] id = new int[]{10};
        map.forEach((k, v) -> {
            id[0]++;
            Types.NestedField optional = optional(id[0], k, v);
            objects.add(optional);
        });
        return new Schema(objects);
    }

    public static Schema createSchema(Map<String, String> map)
    {
        List<ColumnMetadata> columnMetadata = new ArrayList<>();
        TestSchemaMetadata testSchemaMetadata = new TestSchemaMetadata();
        Metadata metadata = testSchemaMetadata.metadata;
        map.forEach((k, v) -> {
            try {
                ColumnMetadata col = new ColumnMetadata(
                        k,
                        metadata.getType(parseTypeSignature(v)),
                        true,
                        null,
                        null,
                        false,
                        Collections.emptyMap());
                columnMetadata.add(col);
            }
            catch (Exception e) {
            }
        });
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (ColumnMetadata column : columnMetadata) {
            if (!column.isHidden()) {
                int index = icebergColumns.size();
                try {
                    org.apache.iceberg.types.Type type3 = toIcebergType(column.getType());
                    Types.NestedField field = Types.NestedField.of(index, column.isNullable(), column.getName(), type3, column.getComment());
                    icebergColumns.add(field);
                }
                catch (Exception e) {
                }
            }
        }
        org.apache.iceberg.types.Type icebergSchema = Types.StructType.of(icebergColumns);
        AtomicInteger nextFieldId = new AtomicInteger(1);
        icebergSchema = TypeUtil.assignFreshIds(icebergSchema, nextFieldId::getAndIncrement);
        return new Schema(icebergSchema.asStructType().fields());
    }

    public static int validatePositive(Object value)
    {
        int intValue = ((Number) value).intValue();
        if (intValue < 0) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, "property must be positive");
        }
        return intValue;
    }

    public static List<PropertyMetadata<?>> getProperty()
    {
        return ImmutableList.of(
                dataSizeProperty(
                        "target_max_file_size",
                        "Target maximum size of written files; the actual size may be larger",
                        new DataSize(8, MEGABYTE),
                        false),
                enumProperty(
                        "orc_writer_validate_mode",
                        "ORC: Level of detail in ORC validation",
                        OrcWriteValidation.OrcWriteValidationMode.class,
                        OrcWriteValidation.OrcWriteValidationMode.BOTH,
                        false),
                dataSizeProperty(
                        "orc_string_statistics_limit",
                        "ORC: Maximum size of string statistics; drop if exceeding",
                        new DataSize(8, MEGABYTE),
                        false),
                dataSizeProperty(
                        "orc_writer_max_dictionary_memory",
                        "ORC: Max dictionary memory",
                        new DataSize(8, MEGABYTE),
                        false),
                integerProperty(
                        "orc_writer_max_stripe_rows",
                        "ORC: Max stripe row count",
                        10,
                        false),
                dataSizeProperty(
                        "orc_writer_max_stripe_size",
                        "ORC: Max stripe size",
                        new DataSize(8, MEGABYTE),
                        false),
                dataSizeProperty(
                        "orc_writer_min_stripe_size",
                        "ORC: Min stripe size",
                        new DataSize(8, MEGABYTE),
                        false),
                enumProperty(
                        "compression_codec",
                        "Compression codec to use when writing files",
                        HiveCompressionCodec.class,
                        ZSTD,
                        false),
                doubleProperty(
                        "orc_writer_validate_percentage",
                        "ORC: Percentage of written files to validate by re-reading them",
                        50.0,
                        doubleValue -> {
                            if (doubleValue < 0.0 || doubleValue > 100.0) {
                                throw new PrestoException(INVALID_SESSION_PROPERTY, format(
                                        "%s must be between 0.0 and 100.0 inclusive: %s",
                                        "orc_writer_validate_percentage",
                                        doubleValue));
                            }
                        },
                        false),
                dataSizeProperty(
                        "parquet_max_read_block_size",
                        "Parquet: Maximum size of a block to read",
                        new DataSize(8, MEGABYTE),
                        false),
                booleanProperty(
                        "orc_bloom_filters_enabled",
                        "ORC: Enable bloom filters for predicate pushdown",
                        true,
                        false),
                booleanProperty(
                        "orc_nested_lazy_enabled",
                        "Experimental: ORC: Lazily read nested data",
                        true,
                        false),
                booleanProperty(
                        "orc_lazy_read_small_ranges",
                        "Experimental: ORC: Read small file segments lazily",
                        true,
                        false),
                PropertyMetadata.dataSizeProperty(
                        "orc_max_read_block_size",
                        "ORC: Soft max size of Presto blocks produced by ORC reader",
                        new DataSize(8, MEGABYTE),
                        false),
                dataSizeProperty(
                        "orc_tiny_stripe_threshold",
                        "ORC: Threshold below which an ORC stripe or file will read in its entirety",
                        new DataSize(8, MEGABYTE),
                        false),
                PropertyMetadata.dataSizeProperty(
                        "orc_stream_buffer_size",
                        "ORC: Size of buffer for streaming reads",
                        new DataSize(8, MEGABYTE),
                        false),
                PropertyMetadata.dataSizeProperty(
                        "orc_max_buffer_size",
                        "ORC: Maximum size of a single read",
                        new DataSize(8, MEGABYTE),
                        false),
                dataSizeProperty(
                        "orc_max_merge_distance",
                        "ORC: Maximum size of gap between two reads to merge into a single read",
                        new DataSize(100, MEGABYTE),
                        false),
                booleanProperty(
                        "statistics_enabled",
                        "test property",
                        true,
                        false),
                booleanProperty(
                        "projection_pushdown_enabled",
                        "test property",
                        true,
                        false),
                new PropertyMetadata<>(
                        "positive_property",
                        "property that should be positive",
                        INTEGER,
                        Integer.class,
                        null,
                        false,
                        value -> validatePositive(value),
                        value -> value));
    }

    public static HiveConfig getHiveConfig()
    {
        return new HiveConfig()
                .setMaxOpenSortFiles(10)
                .setWriterSortBufferSize(new DataSize(100, KILOBYTE));
    }
}
