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
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.Session;
import io.prestosql.cube.CubeManager;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.DropCube;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.cube.CubeManager.STAR_TREE;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_CUBE;

public class DropCubeTask
        implements DataDefinitionTask<DropCube>
{
    private final CubeManager cubeManager;

    @Inject
    public DropCubeTask(CubeManager cubeManager)
    {
        this.cubeManager = cubeManager;
    }

    @Override
    public String getName()
    {
        return "DROP CUBE";
    }

    @Override
    public ListenableFuture<?> execute(DropCube statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl,
            QueryStateMachine stateMachine, List<Expression> parameters, HeuristicIndexerManager heuristicIndexerManager)
    {
        Session session = stateMachine.getSession();
        Optional<CubeMetaStore> optionalCubeMetaStore = this.cubeManager.getMetaStore(STAR_TREE);
        if (!optionalCubeMetaStore.isPresent()) {
            throw new RuntimeException("HetuMetastore is not initialized");
        }
        CubeMetaStore cubeMetaStore = optionalCubeMetaStore.get();
        QualifiedObjectName fullObjectName = createQualifiedObjectName(session, statement, statement.getCubeName());
        QualifiedName cubeTableName = QualifiedName.of(fullObjectName.getCatalogName(), fullObjectName.getSchemaName(), fullObjectName.getObjectName());
        Optional<CubeMetadata> matchedCube = cubeMetaStore.getMetadataFromCubeName(cubeTableName.toString());
        if (!matchedCube.isPresent()) {
            if (!statement.isExists()) {
                throw new SemanticException(MISSING_CUBE, statement, "Cube '%s' does not exist", cubeTableName);
            }
            return immediateFuture(null);
        }
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, fullObjectName);
        tableHandle.ifPresent(handle -> {
            accessControl.checkCanDropTable(session.getRequiredTransactionId(), session.getIdentity(), fullObjectName);
            metadata.dropTable(session, handle);
        });
        cubeMetaStore.removeCube(matchedCube.get());

        return immediateFuture(null);
    }
}
