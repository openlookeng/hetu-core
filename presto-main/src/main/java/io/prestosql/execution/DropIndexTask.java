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
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.DropIndex;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.utils.HeuristicIndexUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
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
            IndexRecord record = indexClient.lookUpIndexRecord(indexName);
            // check indexName exist, call heuristic index api to drop index
            if (record == null) {
                throw new SemanticException(MISSING_INDEX, statement, "Index '%s' does not exist", indexName);
            }

            QualifiedObjectName fullObjectName = QualifiedObjectName.valueOf(record.qualifiedTable);
            Session session = stateMachine.getSession();
            accessControl.checkCanDropIndex(session.getRequiredTransactionId(), session.getIdentity(), fullObjectName);

            List<String> partitions = Collections.emptyList();
            if (statement.getPartitions().isPresent()) {
                partitions = HeuristicIndexUtils.extractPartitions(statement.getPartitions().get());

                List<String> partitionsInindex = indexClient.lookUpIndexRecord(indexName).partitions;
                if (partitionsInindex.isEmpty()) {
                    throw new SemanticException(MISSING_INDEX, statement, "Index '%s' was not created with explicit partitions. Partial drop by partition is not supported.", indexName);
                }

                List<String> missingPartitions = new ArrayList<>(partitions);
                missingPartitions.removeAll(partitionsInindex);
                if (!missingPartitions.isEmpty()) {
                    throw new SemanticException(MISSING_INDEX, statement, "Index '%s' does not contain partitions: %s", indexName, missingPartitions);
                }
            }
            indexClient.deleteIndex(indexName, partitions);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return immediateFuture(null);
    }
}
