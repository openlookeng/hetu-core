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
import io.airlift.units.DataSize;
import io.prestosql.orc.OrcDataSink;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.orc.OrcWriteValidation;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.OrcWriterStats;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.orc.HdfsOrcDataSource;
import io.prestosql.spi.type.BooleanType;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.hetu.core.plugin.iceberg.ColumnIdentity.TypeCategory.ARRAY;
import static io.hetu.core.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.hetu.core.plugin.iceberg.TypeConverter.toOrcType;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.mockito.MockitoAnnotations.initMocks;

public class IcebergOrcFileWriterTest
{
    @Mock
    private Schema mockIcebergSchema;
    @Mock
    private OrcDataSink mockOrcDataSink;
    @Mock
    private Callable<Void> mockRollbackAction;
    @Mock
    private ColumnMetadata<OrcType> mockFileColumnOrcTypes;
    @Mock
    private OrcWriterOptions mockOptions;
    @Mock
    private OrcWriterStats mockStats;
    @Mock
    private FileFormatDataSourceStats fileFormatDataSourceStats;
    @Mock
    private FSDataInputStream mockInputStream;

    private IcebergOrcFileWriter icebergOrcFileWriterUnderTest;

    private static final Column KEY_COLUMN = new Column("a_integer", HIVE_INT, Optional.empty());
    private static final Column DATA_COLUMN = new Column("a_varchar", HIVE_STRING, Optional.empty());
    private static final ColumnIdentity child1 = new ColumnIdentity(1, "child1", PRIMITIVE, ImmutableList.of());
    private static final ColumnIdentity child2 = new ColumnIdentity(2, "child2", PRIMITIVE, ImmutableList.of());
    private static final ColumnIdentity KEY_COLUMN_IDENTITY = new ColumnIdentity(1, KEY_COLUMN.getName(), ARRAY, ImmutableList.of(child1));
    private static final ColumnIdentity DATA_COLUMN_IDENTITY = new ColumnIdentity(2, DATA_COLUMN.getName(), ARRAY, ImmutableList.of(child2));
    private static final Schema TABLE_SCHEMA = new Schema(
            optional(KEY_COLUMN_IDENTITY.getId(), KEY_COLUMN.getName(), Types.IntegerType.get()),
            optional(DATA_COLUMN_IDENTITY.getId(), DATA_COLUMN.getName(), Types.StringType.get()));

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        Optional<Supplier<OrcDataSource>> validationInputFactory = Optional.of(() -> {
            return new HdfsOrcDataSource(new OrcDataSourceId("id"), 0L, new OrcReaderOptions(), mockInputStream, fileFormatDataSourceStats);
        });
        OrcWriterOptions orcWriterOptions = new OrcWriterOptions()
                .withStripeMinSize(new DataSize(0, MEGABYTE))
                .withStripeMaxSize(new DataSize(32, MEGABYTE))
                .withStripeMaxRowCount(30_000)
                .withRowGroupMaxRowCount(10_000)
                .withDictionaryMaxMemory(new DataSize(32, MEGABYTE));

        icebergOrcFileWriterUnderTest = new IcebergOrcFileWriter(
                MetricsConfig.getDefault(),
                mockIcebergSchema,
                mockOrcDataSink,
                mockRollbackAction,
                Arrays.asList("value", "value1"),
                Arrays.asList(BooleanType.BOOLEAN, INTEGER),
                toOrcType(TABLE_SCHEMA),
                CompressionKind.NONE,
                orcWriterOptions,
                new int[]{0},
                new HashMap<>(),
                validationInputFactory,
                OrcWriteValidation.OrcWriteValidationMode.HASHED,
                mockStats);
    }

    @Test
    public void testGetMetrics()
    {
        // Setup
        // Run the test
        icebergOrcFileWriterUnderTest.getMetrics();

        // Verify the results
    }

    @Test
    public void testGetVerificationTask()
    {
        // Setup
        // Run the test
        final Optional<Runnable> result = icebergOrcFileWriterUnderTest.getVerificationTask();

        // Verify the results
    }
}
