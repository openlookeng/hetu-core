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
package io.prestosql.plugin.hive.parquet;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.testing.TestingTypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Properties;

import static io.prestosql.plugin.hive.HiveStorageFormat.PARQUET;
import static io.prestosql.plugin.hive.HiveUtil.shouldUseRecordReaderFromInputFormat;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_OUTPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_SERDE;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestParquetPageSourceFactory
{
    private static final String PARQUET_HIVE_SERDE = "parquet.hive.serde.ParquetHiveSerDe";

    private ParquetPageSourceFactory parquetPageSourceFactory;

    @BeforeClass
    public void setUp()
    {
        HiveHdfsConfiguration hiveHdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(new HiveConfig(), ImmutableSet.of()), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hiveHdfsConfiguration, new HiveConfig(), new NoHdfsAuthentication());
        parquetPageSourceFactory = new ParquetPageSourceFactory(new TestingTypeManager(), hdfsEnvironment, new FileFormatDataSourceStats(), new HiveConfig());
    }

    @AfterClass(alwaysRun = true)
    public void cleanUp()
    {
        parquetPageSourceFactory = null;
    }

    @Test
    public void testCreatePageSourceEmptyWithoutParquetSerDe()
    {
        Properties schema = new Properties();
        schema.setProperty(META_TABLE_SERDE, PARQUET_HIVE_SERDE);
        schema.setProperty(SERIALIZATION_LIB, "");
        schema.setProperty(FILE_INPUT_FORMAT, "");
        schema.setProperty(FILE_OUTPUT_FORMAT, "");
        Optional<? extends ConnectorPageSource> optionalPageSource = parquetPageSourceFactory.createPageSource(new Configuration(), null, null, 0L, 0L, 0L, schema, null, null, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), null, false, -1L);
        assertFalse(optionalPageSource.isPresent());
    }

    @Test
    public void testCreatePageSourceEmptyWithParquetSerDeAndAnnotation()
    {
        Properties schema = new Properties();
        schema.setProperty(META_TABLE_SERDE, PARQUET_HIVE_SERDE);
        schema.setProperty(SERIALIZATION_LIB, PARQUET.getSerDe());
        schema.setProperty(FILE_INPUT_FORMAT, HoodieParquetRealtimeInputFormat.class.getName());
        schema.setProperty(FILE_OUTPUT_FORMAT, "");
        Optional<? extends ConnectorPageSource> optionalPageSource = parquetPageSourceFactory.createPageSource(new Configuration(), null, null, 0L, 0L, 0L, schema, null, null, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), null, false, -1L);
        assertTrue(shouldUseRecordReaderFromInputFormat(new Configuration(), schema));
        assertFalse(optionalPageSource.isPresent());
    }
}
