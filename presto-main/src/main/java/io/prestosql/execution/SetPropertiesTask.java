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
package io.prestosql.execution;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.MaterializedViewPropertyManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TablePropertyManager;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SetProperties;
import io.prestosql.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.prestosql.sql.ParameterUtils.parameterExtractor;
import static io.prestosql.sql.analyzer.SemanticException.semanticException;
import static io.prestosql.sql.tree.SetProperties.Type.MATERIALIZED_VIEW;
import static io.prestosql.sql.tree.SetProperties.Type.TABLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SetPropertiesTask
        implements DataDefinitionTask<SetProperties>
{
    private final TablePropertyManager tablePropertyManager;
    private final MaterializedViewPropertyManager materializedViewPropertyManager;

    @Inject
    public SetPropertiesTask(TablePropertyManager tablePropertyManager, MaterializedViewPropertyManager materializedViewPropertyManager)
    {
        this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
        this.materializedViewPropertyManager = requireNonNull(materializedViewPropertyManager, "materializedViewPropertyManager is null");
    }

    @Override
    public String getName()
    {
        return "SET PROPERTIES";
    }

    @Override
    public ListenableFuture<?> execute(
            SetProperties statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            HeuristicIndexerManager heuristicIndexerManager)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName objectName = createQualifiedObjectName(session, statement, statement.getName());

        if (statement.getType() == TABLE) {
            Map<String, Optional<Object>> properties = tablePropertyManager.getNullableProperties(
                    getRequiredCatalogHandle(metadata, session, statement, objectName.getCatalogName()),
                    statement.getProperties(),
                    session,
                    metadata,
                    accessControl,
                    parameterExtractor(statement, parameters),
                    false);
            setTableProperties(accessControl, metadata, statement, objectName, session, properties);
        }
        else if (statement.getType() == MATERIALIZED_VIEW) {
            Map<String, Optional<Object>> properties = materializedViewPropertyManager.getNullableProperties(
                    getRequiredCatalogHandle(metadata, session, statement, objectName.getCatalogName()),
                    statement.getProperties(),
                    session,
                    metadata,
                    accessControl,
                    parameterExtractor(statement, parameters),
                    false);
            setMaterializedViewProperties(accessControl, metadata, statement, objectName, session, properties);
        }
        else {
            throw semanticException(NOT_SUPPORTED, statement, "Unsupported target type: %s", statement.getType());
        }
        return Futures.immediateFuture(null);
    }

    private void setTableProperties(AccessControl accessControl, Metadata metadata, SetProperties statement, QualifiedObjectName tableName, Session session, Map<String, Optional<Object>> properties)
    {
        if (metadata.isMaterializedView(session, tableName)) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot set properties to a materialized view in ALTER TABLE");
        }

        if (metadata.isView(session, tableName)) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot set properties to a view in ALTER TABLE");
        }

        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (!tableHandle.isPresent()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table does not exist: %s", tableName);
        }
        accessControl.checkCanSetTableProperties(session.toSecurityContext(), tableName, properties);
        metadata.setTableProperties(session, tableHandle.get(), properties);
    }

    private void setMaterializedViewProperties(
            AccessControl accessControl,
            Metadata metadata,
            SetProperties statement,
            QualifiedObjectName materializedViewName,
            Session session,
            Map<String, Optional<Object>> properties)
    {
        if (!metadata.getMaterializedView(session, materializedViewName).isPresent()) {
            String exceptionMessage = format("Materialized View '%s' does not exist", materializedViewName);
            if (metadata.getView(session, materializedViewName).isPresent()) {
                exceptionMessage += ", but a view with that name exists.";
            }
            else if (metadata.getTableHandle(session, materializedViewName).isPresent()) {
                exceptionMessage += ", but a table with that name exists. Did you mean ALTER TABLE " + materializedViewName + " SET PROPERTIES ...?";
            }
            throw semanticException(TABLE_NOT_FOUND, statement, exceptionMessage);
        }
        accessControl.checkCanSetMaterializedViewProperties(session.toSecurityContext(), materializedViewName, properties);
        metadata.setMaterializedViewProperties(session, materializedViewName, properties);
    }
}
