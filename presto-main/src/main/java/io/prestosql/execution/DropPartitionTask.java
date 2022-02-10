/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.DropPartition;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NodeLocation;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;
import io.prestosql.transaction.TransactionManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_TABLE;

public class DropPartitionTask
        implements DataDefinitionTask<DropPartition>
{
    @Override
    public String getName()
    {
        return "DROP PARTITION";
    }

    @Override
    public ListenableFuture<?> execute(DropPartition statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters, HeuristicIndexerManager heuristicIndexerManager)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName fullTableName = createQualifiedObjectName(session, statement, statement.getTableName());
        QualifiedName tableName = QualifiedName.of(fullTableName.getCatalogName(), fullTableName.getSchemaName(), fullTableName.getObjectName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, fullTableName);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, statement, "Table '%s' does not exist", tableName);
        }
        accessControl.checkCanDropPartition(session.getRequiredTransactionId(), session.getIdentity(), fullTableName);
        List<Map<String, String>> partitions = new ArrayList<>();
        List<Map<String, String>> operatorMap = new ArrayList<>();
        Integer index = 0;
        List<List<ComparisonExpression>> partitionSpec = statement.getPartitionSpec();
        for (List<ComparisonExpression> partitionSpecEntry : partitionSpec) {
            partitions.add(index, new HashMap<>());
            operatorMap.add(index, new HashMap<>());
            for (ComparisonExpression comparisonExpression : partitionSpecEntry) {
                partitions.get(index).put(comparisonExpression.getLeft().toString(), extractExpressionValue(comparisonExpression.getRight(), null));
                operatorMap.get(index).put(comparisonExpression.getLeft().toString(), comparisonExpression.getOperator().getValue());
            }
            index++;
        }

        metadata.dropPartition(session, tableHandle.get(), partitions, statement.isIfExists(), operatorMap);
        return immediateFuture(null);
    }

    public String extractExpressionValue(Expression expression, String type)
    {
        if (expression instanceof Cast) {
            return extractExpressionValue(((Cast) expression).getExpression(), ((Cast) expression).getType());
        }
        if (expression instanceof BinaryLiteral) {
            return String.valueOf(((BinaryLiteral) expression).getValue());
        }
        if (expression instanceof BooleanLiteral) {
            return String.valueOf(((BooleanLiteral) expression).getValue());
        }
        if (expression instanceof CharLiteral) {
            return String.valueOf(((CharLiteral) expression).getValue());
        }
        if (expression instanceof DecimalLiteral) {
            return String.valueOf(((DecimalLiteral) expression).getValue());
        }
        if (expression instanceof DoubleLiteral) {
            return String.valueOf(((DoubleLiteral) expression).getValue());
        }
        if (expression instanceof LongLiteral) {
            return String.valueOf(((LongLiteral) expression).getValue());
        }
        if (expression instanceof StringLiteral) {
            String value = ((StringLiteral) expression).getValue();
            Optional<NodeLocation> location = expression.getLocation();
            if ("timestamp".equalsIgnoreCase(type)) {
                TimestampLiteral timestampLiteral = location.isPresent() ? new TimestampLiteral(location.get(), value) : new TimestampLiteral(value);
                return timestampLiteral.getValue();
            }
            if ("date".equalsIgnoreCase(type) || "real".equalsIgnoreCase(type)) {
                GenericLiteral genericLiteral = location.isPresent() ? new GenericLiteral(location.get(), type, value) : new GenericLiteral(type, value);
                return genericLiteral.getValue();
            }
            else if ("decimal".equalsIgnoreCase(type)) {
                DecimalLiteral decimalLiteral = location.isPresent() ? new DecimalLiteral(location.get(), value) : new DecimalLiteral(value);
                return decimalLiteral.getValue();
            }
            return value;
        }
        if (expression instanceof TimeLiteral) {
            return String.valueOf(((TimeLiteral) expression).getValue());
        }
        if (expression instanceof TimestampLiteral) {
            return String.valueOf(((TimestampLiteral) expression).getValue());
        }
        if (expression instanceof GenericLiteral) {
            return ((GenericLiteral) expression).getValue();
        }
        return String.valueOf(expression);
    }
}
