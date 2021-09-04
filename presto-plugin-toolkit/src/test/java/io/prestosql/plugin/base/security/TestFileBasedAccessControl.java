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
package io.prestosql.plugin.base.security;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.ConnectorIdentity;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Set;

import static io.prestosql.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertThrows;

public class TestFileBasedAccessControl
{
    public static final ConnectorTransactionHandle TRANSACTION_HANDLE = new ConnectorTransactionHandle() {};
    private static final ConnectorIdentity ADMIN = user("admin", ImmutableSet.of("admin", "staff"));
    private static final ConnectorIdentity ALICE = user("alice", ImmutableSet.of("staff"));
    private static final ConnectorIdentity BOB = user("bob", ImmutableSet.of("staff"));
    private static final ConnectorIdentity CHARLIE = user("charlie", ImmutableSet.of("guests"));
    private static final ConnectorIdentity JOE = user("joe", ImmutableSet.of());

    @Test
    public void testSchemaRules()
            throws IOException
    {
        ConnectorAccessControl accessControl = createAccessControl("schema.json");
        accessControl.checkCanCreateTable(TRANSACTION_HANDLE, ADMIN, new SchemaTableName("test", "test"));
        accessControl.checkCanCreateTable(TRANSACTION_HANDLE, BOB, new SchemaTableName("bob", "test"));
        assertDenied(() -> accessControl.checkCanCreateTable(TRANSACTION_HANDLE, BOB, new SchemaTableName("test", "test")));
        assertDenied(() -> accessControl.checkCanCreateTable(TRANSACTION_HANDLE, ADMIN, new SchemaTableName("secret", "test")));
    }

    @Test
    public void testTableRules()
            throws IOException
    {
        ConnectorAccessControl accessControl = createAccessControl("table.json");
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, ALICE, new SchemaTableName("test", "test"), ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, ALICE, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, ALICE, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of("bobcolumn"));
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, BOB, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, BOB, new SchemaTableName("bobschema", "bobtable"));
        accessControl.checkCanDeleteFromTable(TRANSACTION_HANDLE, BOB, new SchemaTableName("bobschema", "bobtable"));
        accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, JOE, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanCreateViewWithSelectFromColumns(TRANSACTION_HANDLE, BOB, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of());
        accessControl.checkCanDropTable(TRANSACTION_HANDLE, ADMIN, new SchemaTableName("bobschema", "bobtable"));
        assertDenied(() -> accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, ALICE, new SchemaTableName("bobschema", "bobtable")));
        assertDenied(() -> accessControl.checkCanDropTable(TRANSACTION_HANDLE, BOB, new SchemaTableName("bobschema", "bobtable")));
        assertDenied(() -> accessControl.checkCanInsertIntoTable(TRANSACTION_HANDLE, BOB, new SchemaTableName("test", "test")));
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, ADMIN, new SchemaTableName("secret", "secret"), ImmutableSet.of()));
        assertDenied(() -> accessControl.checkCanSelectFromColumns(TRANSACTION_HANDLE, JOE, new SchemaTableName("secret", "secret"), ImmutableSet.of()));
        assertDenied(() -> accessControl.checkCanCreateViewWithSelectFromColumns(TRANSACTION_HANDLE, JOE, new SchemaTableName("bobschema", "bobtable"), ImmutableSet.of()));
    }

    @Test
    public void testSessionPropertyRules()
            throws IOException
    {
        ConnectorAccessControl accessControl = createAccessControl("session_property.json");
        accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, ADMIN, "dangerous");
        accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, ALICE, "safe");
        accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, ALICE, "unsafe");
        accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, BOB, "safe");
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, BOB, "unsafe"));
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, ALICE, "dangerous"));
        assertDenied(() -> accessControl.checkCanSetCatalogSessionProperty(TRANSACTION_HANDLE, CHARLIE, "safe"));
    }

    @Test
    public void testInvalidRules()
    {
        assertThatThrownBy(() -> createAccessControl("invalid.json"))
                .hasMessageContaining("Invalid JSON");
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorAccessControl.class, FileBasedAccessControl.class);
    }

    private static ConnectorIdentity user(String name, Set<String> groups)
    {
        return ConnectorIdentity.forUser(name).withGroups(groups).build();
    }

    private ConnectorAccessControl createAccessControl(String fileName)
            throws IOException
    {
        String path = this.getClass().getClassLoader().getResource(fileName).getPath();
        FileBasedAccessControlConfig config = new FileBasedAccessControlConfig();
        config.setConfigFile(path);
        return new FileBasedAccessControl(config);
    }

    private static void assertDenied(ThrowingRunnable runnable)
    {
        assertThrows(AccessDeniedException.class, runnable);
    }
}
