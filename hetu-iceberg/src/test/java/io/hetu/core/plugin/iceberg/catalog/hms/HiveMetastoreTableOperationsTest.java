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
package io.hetu.core.plugin.iceberg.catalog.hms;

import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastore;
import io.prestosql.spi.connector.ConnectorSession;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class HiveMetastoreTableOperationsTest
{
    @Mock
    private FileIO mockFileIo;
    @Mock
    private HiveMetastore mockMetastore;
    @Mock
    private ThriftMetastore mockThriftMetastore;
    @Mock
    private ConnectorSession mockSession;

    private HiveMetastoreTableOperations hiveMetastoreTableOperationsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        hiveMetastoreTableOperationsUnderTest = new HiveMetastoreTableOperations(mockFileIo, mockMetastore,
                mockThriftMetastore, mockSession, "database", "table",
                Optional.of("value"), Optional.of("value"));
    }

    @Test
    public void testCommitToExistingTable()
    {
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", new Types.StringType())), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.optional(0, "name", new Types.StringType())), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        // Run the test
        hiveMetastoreTableOperationsUnderTest.commitToExistingTable(base, metadata);
    }
}
