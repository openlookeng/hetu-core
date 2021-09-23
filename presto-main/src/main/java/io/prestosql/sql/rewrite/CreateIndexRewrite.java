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
import io.prestosql.security.AccessControl;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.CreateIndex;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.Statement;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.sql.ParsingUtil.createParsingOptions;
import static java.util.Objects.requireNonNull;

public class CreateIndexRewrite
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
        return (Statement) new Visitor(parser, session).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final SqlParser sqlParser;

        public Visitor(SqlParser sqlParser, Session session)
        {
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        protected Node visitCreateIndex(CreateIndex createIndex, Void context)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("select ");

            if (createIndex.getColumnAliases() == null) {
                builder.append("* ");
            }
            else {
                List<String> columns = createIndex.getColumnAliases().stream()
                        .map(Identifier::getValue).collect(Collectors.toList());
                builder.append(String.join(", ", columns));
            }

            builder.append(" from ");
            builder.append(createIndex.getTableName());
            if (createIndex.getExpression().isPresent()) {
                builder.append(" where");
                String whereStr = createIndex.getExpression().get().toString();
                builder.append(" ").append(whereStr, 1, whereStr.length() - 1);
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
