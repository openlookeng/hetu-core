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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.hetu.core.plugin.iceberg.util.IcebergTestUtil;
import io.prestosql.icebergutil.TestSchemaMetadata;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.orc.OrcWriteValidation;
import io.prestosql.parquet.writer.ParquetWriterOptions;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveCompressionCodec;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.orc.OrcWriterConfig;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.testing.TestingTypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetWriter;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.hetu.core.plugin.iceberg.util.IcebergTestUtil.getSession;
import static io.hetu.core.plugin.iceberg.util.IcebergTestUtil.validatePositive;
import static io.prestosql.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.prestosql.plugin.hive.HiveCompressionCodec.ZSTD;
import static io.prestosql.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;
import static io.prestosql.spi.session.PropertyMetadata.doubleProperty;
import static io.prestosql.spi.session.PropertyMetadata.enumProperty;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static org.mockito.MockitoAnnotations.initMocks;

public class IcebergFileWriterFactoryTest
{
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;
    @Mock
    private TypeManager mockTypeManager;
    @Mock
    private FileFormatDataSourceStats mockReadStats;
    @Mock
    private OrcWriterConfig mockOrcWriterConfig;

    private IcebergFileWriterFactory icebergFileWriterFactoryUnderTest;
    private TestSchemaMetadata metadata;
    private ConnectorSession connectorSession;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);

        mockTypeManager = new TestingTypeManager();
        metadata = IcebergTestUtil.getMetadata();
        SessionPropertyManager sessionPropertyManager = metadata.metadata.getSessionPropertyManager();
        HashMap<CatalogName, Map<String, String>> catalogNameMapHashMap = new HashMap<>();
        CatalogName testCatalog = new CatalogName("test_catalog");
        catalogNameMapHashMap.put(testCatalog, ImmutableMap.of("projection_pushdown_enabled", "true",
                "statistics_enabled", "true",
                "orc_writer_validate_percentage", "100d",
                "orc_writer_min_stripe_size", "462304B"));
        List<PropertyMetadata<?>> sessionProperties = ImmutableList.of(
                integerProperty(
                        "parquet_writer_batch_size",
                        "Parquet: Maximum number of rows passed to the writer in each batch",
                        ParquetWriterOptions.DEFAULT_BATCH_SIZE,
                        false),
                PropertyMetadata.dataSizeProperty(
                        "parquet_writer_block_size",
                        "Parquet: Writer block size",
                        new DataSize(ParquetWriter.DEFAULT_BLOCK_SIZE, BYTE),
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
                        DataSize.valueOf("562304B"),
                        false),
                dataSizeProperty(
                        "orc_writer_max_dictionary_memory",
                        "ORC: Max dictionary memory",
                        DataSize.valueOf("562304B"),
                        false),
                integerProperty(
                        "orc_writer_max_stripe_rows",
                        "ORC: Max stripe row count",
                        10,
                        false),
                dataSizeProperty(
                        "orc_writer_max_stripe_size",
                        "ORC: Max stripe size",
                        DataSize.valueOf("562304B"),
                        false),
                dataSizeProperty(
                        "orc_writer_min_stripe_size",
                        "ORC: Min stripe size",
                        DataSize.valueOf("462304B"),
                        false),
                enumProperty(
                        "compression_codec",
                        "Compression codec to use when writing files",
                        HiveCompressionCodec.class,
                        ZSTD,
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
                        value -> value),
                dataSizeProperty(
                        "parquet_writer_page_size",
                        "Parquet: Writer page size",
                        DataSize.valueOf("462304B"),
                        false),
                doubleProperty(
                        "orc_writer_validate_percentage",
                        "ORC: Percentage of written files to validate by re-reading them",
                        100d,
                        doubleValue -> {
                            if (doubleValue < 0.0 || doubleValue > 100.0) {
                                throw new PrestoException(INVALID_SESSION_PROPERTY, format(
                                        "%s must be between 0.0 and 100.0 inclusive: %s",
                                        "orc_writer_validate_percentage",
                                        doubleValue));
                            }
                        },
                        false));
        sessionPropertyManager.addConnectorSessionProperties(testCatalog, sessionProperties);
        connectorSession = IcebergTestUtil.getConnectorSession3(getSession(), sessionPropertyManager, catalogNameMapHashMap, metadata);

        icebergFileWriterFactoryUnderTest = new IcebergFileWriterFactory(mockHdfsEnvironment, mockTypeManager, new NodeVersion("version"), new FileFormatDataSourceStats(), new OrcWriterConfig());
    }

    @Test
    public void testGetOrcWriterStats()
    {
        icebergFileWriterFactoryUnderTest.getOrcWriterStats();
    }

    @Test
    public void testCreateDataFileWriterOrc()
    {
        Schema schema = new Schema(0, Arrays.asList(Types.NestedField.required(0, "name", new Types.UUIDType())), new HashMap<>(), new HashSet<>(Arrays.asList(0)));
        ImmutableMap<String, String> of = ImmutableMap.of(
                "orc.bloom.filter.columns", "1",
                "orc.bloom.filter.fpp", "2");
        IcebergFileWriter dataFileWriter = icebergFileWriterFactoryUnderTest.createDataFileWriter(
                null,
                schema,
                new JobConf(IcebergFileWriterFactory.class),
                connectorSession,
                new HdfsEnvironment.HdfsContext(new HdfsFileIoProviderTest.ConnectorSession(), "test_schema", "test_table"),
                IcebergFileFormat.ORC,
                MetricsConfig.getDefault(),
                of);
    }

    @Test
    public void testCreateDataFileWriterParqut()
    {
        Path path = new Path("/home", "/home/child");
        Schema schema = new Schema(0, Arrays.asList(Types.NestedField.required(0, "name", new Types.UUIDType())), new HashMap<>(), new HashSet<>(Arrays.asList(0)));
        IcebergFileWriter dataFileWriter = icebergFileWriterFactoryUnderTest.createDataFileWriter(
                path,
                schema,
                new JobConf(IcebergFileWriterFactory.class),
                connectorSession,
                new HdfsEnvironment.HdfsContext(new HdfsFileIoProviderTest.ConnectorSession(), "test_schema", "test_table"),
                IcebergFileFormat.PARQUET,
                MetricsConfig.getDefault(),
                new HashMap<>());
    }
}
