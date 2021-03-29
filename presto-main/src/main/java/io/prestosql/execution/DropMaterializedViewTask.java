/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.DropMaterializedView;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;

public class DropMaterializedViewTask
        implements DataDefinitionTask<DropMaterializedView>
{
    @Override
    public String getName()
    {
        return "DROP MATERIALIZED VIEW";
    }

    @Override
    public ListenableFuture<?> execute(DropMaterializedView statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl,
            QueryStateMachine stateMachine, List<Expression> parameters, HeuristicIndexerManager heuristicIndexerManager)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTableName());

//         materialized view exists?
//        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        Optional<ConnectorViewDefinition> definition = metadata.getView(session, tableName);
        if (!definition.isPresent()) {
            if (!statement.isExists()) {
                throw new SemanticException(MISSING_TABLE, statement, "MATERIALIZED VIEW '%s' does not exist", tableName);
            }
            return immediateFuture(null);
        }

        if (!(definition.get() instanceof ConnectorMaterializedViewDefinition)) {
            throw new SemanticException(NOT_SUPPORTED, statement, "'%s' is not a materialized view.", tableName);
        }

        accessControl.checkCanDropMaterializedTable(session.getRequiredTransactionId(), session.getIdentity(), tableName);

        metadata.dropMaterializedView(session, (ConnectorMaterializedViewDefinition) definition.get(), tableName);

        return immediateFuture(null);
    }
}
