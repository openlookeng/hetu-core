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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableSet;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.s3.PrestoS3ClientFactory;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.mockito.MockitoAnnotations.initMocks;

public class S3SelectRecordCursorProviderTest
{
    @Mock
    private HdfsEnvironment mockHdfsEnvironment;
    @Mock
    private HiveConfig mockHiveConfig;
    @Mock
    private PrestoS3ClientFactory mockS3ClientFactory;

    private S3SelectRecordCursorProvider s3SelectRecordCursorProviderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        HiveConfig hdfsConfig = new HiveConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        s3SelectRecordCursorProviderUnderTest = new S3SelectRecordCursorProvider(hdfsEnvironment, hdfsConfig,
                new PrestoS3ClientFactory());
    }

    @Test
    public void testCreateRecordCursor() throws Exception
    {
        // Setup
        final Configuration configuration = new Configuration(false);
        final ConnectorSession session = new VacuumCleanerTest.ConnectorSession();
        final Path path = new Path("scheme");
        final Properties schema = new Properties();
        final List<HiveColumnHandle> columns = Arrays.asList(
                new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false));
        final TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.withColumnDomains(new HashMap<>());
        final TypeManager typeManager = FunctionAndTypeManager.createTestFunctionAndTypeManager();
        final Map<String, String> customSplitInfo = new HashMap<>();

        // Run the test
//        final Optional<RecordCursor> result = s3SelectRecordCursorProviderUnderTest.createRecordCursor(
//                configuration,
//                session,
//                path,
//                0L,
//                0L,
//                0L,
//                schema,
//                columns,
//                effectivePredicate,
//                typeManager,
//                false,
//                customSplitInfo);
        s3SelectRecordCursorProviderUnderTest.createRecordCursor(
                configuration,
                session,
                path,
                0L,
                0L,
                0L,
                schema,
                columns,
                effectivePredicate,
                typeManager,
                true,
                customSplitInfo);
    }

    @Test
    public void testCreateRecordCursor_HdfsEnvironmentThrowsIOException() throws Exception
    {
        // Setup
        final Configuration configuration = new Configuration(false);
        final ConnectorSession session = null;
        final Path path = new Path("scheme", "authority", "path");
        final Properties schema = new Properties();
        final List<HiveColumnHandle> columns = Arrays.asList(
                new HiveColumnHandle("name", HiveType.valueOf("hiveTypeName"),
                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false));
        final TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.withColumnDomains(new HashMap<>());
        final TypeManager typeManager = null;
        final Map<String, String> customSplitInfo = new HashMap<>();
        // Run the test
        final Optional<RecordCursor> result = s3SelectRecordCursorProviderUnderTest.createRecordCursor(configuration,
                session, path, 0L, 0L, 0L, schema, columns, effectivePredicate, typeManager, false, customSplitInfo);

        // Verify the results
    }
}
