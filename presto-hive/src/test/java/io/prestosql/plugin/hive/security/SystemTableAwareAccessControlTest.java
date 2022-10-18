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

import io.prestosql.plugin.hive.util.FieldSetterFactoryTest;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.spi.security.ViewExpression;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SystemTableAwareAccessControlTest
{
    @Mock
    private ConnectorAccessControl mockDelegate;

    private SystemTableAwareAccessControl systemTableAwareAccessControlUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        systemTableAwareAccessControlUnderTest = new SystemTableAwareAccessControl(mockDelegate);
    }

    @Test
    public void testDelegate() throws Exception
    {
        // Setup
        // Run the test
        final ConnectorAccessControl result = systemTableAwareAccessControlUnderTest.delegate();

        // Verify the results
    }

    @Test
    public void testCheckCanShowColumnsMetadata()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        systemTableAwareAccessControlUnderTest.checkCanShowColumnsMetadata(transactionHandle, identity, tableName);
    }

    @Test
    public void testFilterColumns() throws Exception
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        final List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("name", new FieldSetterFactoryTest.Type(), false, "comment", "extraInfo", false, new HashMap<>(), false));
        final List<ColumnMetadata> expectedResult = Arrays.asList(
                new ColumnMetadata("name", new FieldSetterFactoryTest.Type(), false, "comment", "extraInfo", false, new HashMap<>(), false));

        // Configure ConnectorAccessControl.filterColumns(...).
        final List<ColumnMetadata> columnMetadata = Arrays.asList(
                new ColumnMetadata("name", new FieldSetterFactoryTest.Type(), false, "comment", "extraInfo", false, new HashMap<>(), false));
        when(mockDelegate.filterColumns(any(ConnectorTransactionHandle.class),
                eq(new ConnectorIdentity("user", new HashSet<>(
                        Arrays.asList("value")), Optional.empty(),
                        Optional.of(new SelectedRole(SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>())),
                eq(new SchemaTableName("schemaName", "tableName")),
                eq(Arrays.asList(
                        new ColumnMetadata("name", new FieldSetterFactoryTest.Type(), false, "comment", "extraInfo", false, new HashMap<>(),
                                false))))).thenReturn(columnMetadata);

        // Run the test
        final List<ColumnMetadata> result = systemTableAwareAccessControlUnderTest.filterColumns(transactionHandle,
                identity, tableName, columns);
    }

    @Test
    public void testCheckCanSelectFromColumns()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        systemTableAwareAccessControlUnderTest.checkCanSelectFromColumns(transactionHandle, identity, tableName,
                new HashSet<>(
                        Arrays.asList("value")));
    }

    @Test
    public void testCheckCanSelectFromColumns_ConnectorAccessControlThrowsAccessDeniedException()
    {
        // Setup
        final ConnectorTransactionHandle transactionHandle = null;
        final ConnectorIdentity identity = new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")),
                Optional.empty(), Optional.of(new SelectedRole(
                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");
        doThrow(AccessDeniedException.class).when(mockDelegate).checkCanSelectFromColumns(
                any(ConnectorTransactionHandle.class),
                eq(new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                        Optional.of(new SelectedRole(
                                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>())),
                eq(new SchemaTableName("schemaName", "tableName")), eq(new HashSet<>(
                        Arrays.asList("value"))));

        // Run the test
        systemTableAwareAccessControlUnderTest.checkCanSelectFromColumns(transactionHandle, identity, tableName,
                new HashSet<>(
                        Arrays.asList("value")));

        // Verify the results
    }

    @Test
    public void testGetRowFilter()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final Optional<ViewExpression> result = systemTableAwareAccessControlUnderTest.getRowFilter(null, identity,
                tableName);

        // Verify the results
    }

    @Test
    public void testGetColumnMask()
    {
        // Setup
        final Identity identity = new Identity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                new HashMap<>(), new HashMap<>());
        final SchemaTableName tableName = new SchemaTableName("schemaName", "tableName");

        // Run the test
        final Optional<ViewExpression> result = systemTableAwareAccessControlUnderTest.getColumnMask(null, identity,
                tableName, "columnName", new FieldSetterFactoryTest.Type());

        // Verify the results
    }
}
