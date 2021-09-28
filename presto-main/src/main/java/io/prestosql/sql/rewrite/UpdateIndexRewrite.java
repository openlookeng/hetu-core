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
package io.prestosql.sql.rewrite;

import io.prestosql.Session;
import io.prestosql.cube.CubeManager;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.UpdateIndex;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static io.prestosql.sql.ParsingUtil.createParsingOptions;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_INDEX;
import static java.util.Objects.requireNonNull;

public class UpdateIndexRewrite
        implements StatementRewrite.Rewrite
{
    @Override
    public Statement rewrite(
            Session session,
            Metadata metadata,
            CubeManager cubeManager,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node,
            List<Expression> parameters,
            AccessControl accessControl,
            WarningCollector warningCollector,
            HeuristicIndexerManager heuristicIndexerManager)
    {
        return (Statement) new UpdateIndexRewrite.Visitor(parser, session, heuristicIndexerManager, metadata, node).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final SqlParser sqlParser;
        private final HeuristicIndexerManager heuristicIndexerManager;
        private final Metadata metadata;
        private final Node node;

        public Visitor(SqlParser sqlParser, Session session, HeuristicIndexerManager heuristicIndexerManager, Metadata metadata, Node node)
        {
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.heuristicIndexerManager = heuristicIndexerManager;
            this.metadata = metadata;
            this.node = node;
        }

        @Override
        protected Node visitUpdateIndex(UpdateIndex updateIndex, Void context)
        {
            IndexRecord indexRecord;
            try {
                indexRecord = heuristicIndexerManager.getIndexClient().lookUpIndexRecord(updateIndex.getIndexName().toString());
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading index records, ", e);
            }
            if (indexRecord == null) {
                throw new SemanticException(MISSING_INDEX, updateIndex, "Index '%s' does not exist", updateIndex.getIndexName().toString());
            }

            StringBuilder builder = new StringBuilder();
            builder.append("select " + String.join(", ", indexRecord.columns) + " from " + indexRecord.qualifiedTable);

            List<String> partitions = indexRecord.partitions;
            if (partitions != null && !partitions.isEmpty()) {
                QualifiedObjectName tableFullName = QualifiedObjectName.valueOf(indexRecord.qualifiedTable);
                Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableFullName);
                if (!tableHandle.isPresent()) {
                    throw new SemanticException(MISSING_ATTRIBUTE, updateIndex, "Table '%s' is invalid", tableFullName);
                }

                String partitionColumnName = partitions.get(0).split("=")[0];
                TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle.get());
                ColumnMetadata partitionColumn = tableMetadata.getColumn(partitionColumnName);
                String valuePrefix = partitionColumn.getType().getTypeSignature().getBase();

                builder.append(" where ");
                builder.append(partitionColumnName);
                builder.append(" IN (");
                for (int i = 0; i < partitions.size(); i++) {
                    String partition = partitions.get(i);
                    String partitionValue = partition.split("=")[1];
                    builder.append(valuePrefix);
                    builder.append(" '");
                    builder.append(partitionValue);
                    builder.append("'");
                    if (i < partitions.size() - 1) {
                        builder.append(",");
                    }
                }

                builder.append(")");
            }

            return sqlParser.createStatement(builder.toString(), createParsingOptions(session));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
