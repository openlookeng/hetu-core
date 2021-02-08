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
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.DropIndex;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.TransactionManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_INDEX;

public class DropIndexTask
        implements DataDefinitionTask<DropIndex>
{
    @Override
    public String getName()
    {
        return "DROP INDEX";
    }

    @Override
    public ListenableFuture<?> execute(DropIndex statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters, HeuristicIndexerManager heuristicIndexerManager)
    {
        IndexClient indexClient = heuristicIndexerManager.getIndexClient();
        String indexName = statement.getIndexName().toString();

        try {
            // check indexName exist, call heuristic index api to drop index
            if (indexClient.getIndexRecord(indexName) == null) {
                throw new SemanticException(MISSING_INDEX, statement, "Index '%s' does not exists", indexName);
            }
            indexClient.deleteIndex(indexName);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return immediateFuture(null);
    }
}
