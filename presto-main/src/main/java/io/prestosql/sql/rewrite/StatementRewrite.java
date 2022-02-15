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
package io.prestosql.sql.rewrite;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.cube.CubeManager;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Statement;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class StatementRewrite
{
    private static final List<Rewrite> REWRITES = ImmutableList.of(
            new DescribeInputRewrite(),
            new DescribeOutputRewrite(),
            new ShowQueriesRewrite(),
            new ShowStatsRewrite(),
            new ExplainRewrite(),
            new CacheTableRewrite(),
            new CreateIndexRewrite(),
            new InsertCubeRewrite(),
            new UpdateIndexRewrite());

    private StatementRewrite() {}

    public static Statement rewrite(
            Session session,
            Metadata metadata,
            CubeManager cubeManager,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement inputNode,
            List<Expression> parameters,
            AccessControl accessControl,
            WarningCollector warningCollector,
            HeuristicIndexerManager heuristicIndexerManager)
    {
        Statement node = inputNode;
        for (Rewrite rewrite : REWRITES) {
            node = requireNonNull(rewrite.rewrite(session, metadata, cubeManager, parser, queryExplainer, node, parameters, accessControl, warningCollector, heuristicIndexerManager),
                    "Statement rewrite returned null");
        }
        return node;
    }

    interface Rewrite
    {
        Statement rewrite(
                Session session,
                Metadata metadata,
                CubeManager cubeManager,
                SqlParser parser,
                Optional<QueryExplainer> queryExplainer,
                Statement node,
                List<Expression> parameters,
                AccessControl accessControl,
                WarningCollector warningCollector,
                HeuristicIndexerManager heuristicIndexerManager);
    }
}
