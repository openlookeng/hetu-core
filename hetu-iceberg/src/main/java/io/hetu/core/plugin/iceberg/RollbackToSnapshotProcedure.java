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
import io.hetu.core.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.procedure.Procedure;
import org.apache.iceberg.Table;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class RollbackToSnapshotProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle ROLLBACK_TO_SNAPSHOT = methodHandle(
            RollbackToSnapshotProcedure.class,
            "rollbackToSnapshot",
            ConnectorSession.class,
            String.class,
            String.class,
            Long.class);

    private final TrinoCatalogFactory catalogFactory;
    private final ClassLoader classLoader;

    @Inject
    public RollbackToSnapshotProcedure(TrinoCatalogFactory catalogFactory)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.classLoader = getClass().getClassLoader();
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "rollback_to_snapshot",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA", VARCHAR.getTypeSignature()),
                        new Procedure.Argument("TABLE", VARCHAR.getTypeSignature()),
                        new Procedure.Argument("SNAPSHOT_ID", BIGINT.getTypeSignature())),
                ROLLBACK_TO_SNAPSHOT.bindTo(this));
    }

    public void rollbackToSnapshot(ConnectorSession clientSession, String schema, String table, Long snapshotId)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            SchemaTableName schemaTableName = new SchemaTableName(schema, table);
            Table icebergTable = catalogFactory.create(clientSession.getIdentity()).loadTable(clientSession, schemaTableName);
            icebergTable.manageSnapshots().setCurrentSnapshot(snapshotId).commit();
        }
    }
}
