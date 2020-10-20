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
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.DropCache;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.transaction.TransactionManager;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_CACHE;

public class DropCacheTask
        implements DataDefinitionTask<DropCache>
{
    @Override
    public String getName()
    {
        return "DROP CACHE";
    }

    @Override
    public ListenableFuture<?> execute(DropCache statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters, HeuristicIndexerManager heuristicIndexerManager)
    {
        if (!PropertyService.getBooleanProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED)) {
            throw new PrestoException(GENERIC_USER_ERROR, "Cache table feature is not enabled");
        }
        Session session = stateMachine.getSession();
        QualifiedObjectName fullObjectName = createQualifiedObjectName(session, statement, statement.getTableName());
        QualifiedName tableName = QualifiedName.of(fullObjectName.getCatalogName(), fullObjectName.getSchemaName(), fullObjectName.getObjectName());
        SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();
        // Check if split cache has predicates for the requested table
        if (!splitCacheMap.cacheExists(tableName)) {
            throw new SemanticException(MISSING_CACHE, statement, "Cache for table '%s' does not exist", tableName.toString());
        }
        splitCacheMap.dropCache(tableName, statement.getWhere().map(Expression::toString));
        return immediateFuture(null);
    }
}
