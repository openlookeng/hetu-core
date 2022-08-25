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

import io.hetu.core.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.SelectedRole;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertThrows;

public class RollbackToSnapshotProcedureTest
{
    @Mock
    private TrinoCatalogFactory mockCatalogFactory;

    private RollbackToSnapshotProcedure rollbackToSnapshotProcedureUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        rollbackToSnapshotProcedureUnderTest = new RollbackToSnapshotProcedure(mockCatalogFactory);
    }

    @Test
    public void testGet()
    {
        // Setup
        // Run the test
        final Procedure result = rollbackToSnapshotProcedureUnderTest.get();

        // Verify the results
    }

    @Test
    public void testGet_ThrowsRuntimeException()
    {
        // Setup
        // Run the test
        assertThrows(RuntimeException.class, () -> rollbackToSnapshotProcedureUnderTest.get());
    }

    @Test
    public void testRollbackToSnapshot()
    {
        // Setup
        final ConnectorSession clientSession = null;
        when(mockCatalogFactory.create(
                new ConnectorIdentity("user", new HashSet<>(Arrays.asList("value")), Optional.empty(),
                        Optional.of(new SelectedRole(
                                SelectedRole.Type.ROLE, Optional.of("value"))), new HashMap<>()))).thenReturn(null);

        // Run the test
        rollbackToSnapshotProcedureUnderTest.rollbackToSnapshot(clientSession, "schema", "table", 0L);

        // Verify the results
    }
}
