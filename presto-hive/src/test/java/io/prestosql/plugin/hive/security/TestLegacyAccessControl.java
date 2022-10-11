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
package io.prestosql.plugin.hive.security;

import io.prestosql.connector.system.GlobalSystemTransactionHandle;
import io.prestosql.plugin.hive.HiveTransactionHandle;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.util.FieldSetterFactoryTest;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.Identity;
import io.prestosql.transaction.TransactionId;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.function.Function;

import static io.prestosql.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestLegacyAccessControl
{
    @Mock
    private Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> function;
    private LegacyAccessControl legacyAccessControlTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        legacyAccessControlTest = new LegacyAccessControl(function, new LegacySecurityConfig());
    }

    @Test
    public void testFilterTables()
    {
        legacyAccessControlTest.filterTables(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                ConnectorIdentity.ofUser("user"),
                new HashSet<>());
    }

    @Test
    public void testFilterColumns()
    {
        ColumnMetadata name = new ColumnMetadata("name", new FieldSetterFactoryTest.Type());
        legacyAccessControlTest.filterColumns(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                ConnectorIdentity.ofUser("user"),
                SchemaTableName.schemaTableName("schemaname", "tablename"),
                Arrays.asList(name));
    }

    @Test
    public void testGetRowFilter()
    {
        legacyAccessControlTest.getRowFilter(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                new Identity("user", Optional.empty()),
                SchemaTableName.schemaTableName("schemaname", "tablename"));
    }

    @Test
    public void testGetColumnMask()
    {
        legacyAccessControlTest.getColumnMask(
                new GlobalSystemTransactionHandle(TransactionId.create()),
                new Identity("user", Optional.empty()),
                SchemaTableName.schemaTableName("schemaname", "tablename"),
                "columnName",
                new FieldSetterFactoryTest.Type());
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorAccessControl.class, LegacyAccessControl.class);
    }
}
