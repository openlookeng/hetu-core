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

import com.google.common.collect.ImmutableSet;
import io.prestosql.parquet.writer.ParquetWriterOptions;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.authentication.GenericExceptionAction;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Callable;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class IcebergParquetFileWriterTest
{
    @Mock
    private Callable<Void> mockRollbackAction;
    @Mock
    private ParquetWriterOptions mockParquetWriterOptions;
    @Mock
    private Path mockOutputPath;
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;

    private IcebergParquetFileWriter icebergParquetFileWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        HiveConfig hiveConfig = new HiveConfig().setMaxInitialSplits(100);
        HdfsConfigurationInitializer updator = new HdfsConfigurationInitializer(hiveConfig);
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(updator, ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, null, new NoHdfsAuthentication());
        GroupType groupType = new GroupType(OPTIONAL, "my_array", new GroupType(REPEATED, "bag", new PrimitiveType(OPTIONAL, INT32, "array_element")));
        icebergParquetFileWriterUnderTest = new IcebergParquetFileWriter(
                new ByteArrayOutputStream(),
                mockRollbackAction,
                Arrays.asList(new IcebergTableHandleTest.Type()),
                new MessageType("name", groupType),
                new HashMap<>(),
                mockParquetWriterOptions,
                new int[]{0},
                CompressionCodecName.UNCOMPRESSED, "trinoVersion", mockOutputPath, hdfsEnvironment,
                new HdfsEnvironment.HdfsContext(new HdfsInputFileTest.ConnectorSession(), "schemaName", "tableName"));
    }

    @Test
    public void testGetMetrics()
    {
        // Setup
        // Configure HdfsEnvironment.doAs(...).
        final Metrics metrics = new Metrics(0L, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(),
                new HashMap<>(), new HashMap<>());
        when(mockHdfsEnvironment.doAs(eq("user"), any(GenericExceptionAction.class))).thenReturn(metrics);

        // Run the test
        final Metrics result = icebergParquetFileWriterUnderTest.getMetrics();

        // Verify the results
    }

    @Test
    public void testGetMetrics_HdfsEnvironmentThrowsE()
    {
        // Setup
        when(mockHdfsEnvironment.doAs(eq("user"), any(GenericExceptionAction.class))).thenThrow(Exception.class);

        // Run the test
        final Metrics result = icebergParquetFileWriterUnderTest.getMetrics();

        // Verify the results
    }
}
