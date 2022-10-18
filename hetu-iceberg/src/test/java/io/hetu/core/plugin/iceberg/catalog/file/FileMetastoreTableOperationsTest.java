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
package io.hetu.core.plugin.iceberg.catalog.file;

import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.connector.ConnectorSession;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class FileMetastoreTableOperationsTest
{
    @Mock
    private FileIO mockFileIo;
    @Mock
    private HiveMetastore mockMetastore;
    @Mock
    private ConnectorSession mockSession;

    private FileMetastoreTableOperations fileMetastoreTableOperationsUnderTest;

    @BeforeMethod
    public void setUp()
    {
        initMocks(this);
        fileMetastoreTableOperationsUnderTest = new FileMetastoreTableOperations(mockFileIo, mockMetastore, mockSession,
                "database", "table",
                Optional.of("value"), Optional.of("value"));
    }

    @Test
    public void testCommitToExistingTable()
    {
        Type type = new Type() {
            @Override
            public TypeID typeId()
            {
                return TypeID.STRING;
            }
        };
        Types.NestedField name = Types.NestedField.required(0, "name", type);
        // Setup
        final TableMetadata base = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(Types.NestedField.required(0, "name", type)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        final TableMetadata metadata = TableMetadata.newTableMetadata(new Schema(0, Arrays.asList(
                        Types.NestedField.required(0, "name", type)), new HashMap<>(), new HashSet<>(Arrays.asList(0))),
                PartitionSpec.unpartitioned(), "location", new HashMap<>());
        // Run the test
        fileMetastoreTableOperationsUnderTest.commitToExistingTable(base, metadata);
    }
}
