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

package io.prestosql.sql.rewrite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.aggregator.AggregationSignature;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.Session;
import io.prestosql.cube.CubeManager;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GroupBy;
import io.prestosql.sql.tree.GroupingSets;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.InsertCube;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static io.prestosql.cube.CubeManager.STAR_TREE;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static java.util.Objects.requireNonNull;

public class InsertCubeRewrite
        implements StatementRewrite.Rewrite
{
    @Override
    public Statement rewrite(
            Session session,
            Metadata metadata,
            CubeManager cubeManager,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node, List<Expression> parameters,
            AccessControl accessControl,
            WarningCollector warningCollector,
            HeuristicIndexerManager heuristicIndexerManager)
    {
        return (Statement) new Visitor(session, cubeManager).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final CubeManager cubeManager;

        public Visitor(Session session, CubeManager cubeManager)
        {
            this.session = requireNonNull(session, "session is null");
            this.cubeManager = requireNonNull(cubeManager, "cubeManager is null");
        }

        @Override
        protected Node visitInsertCube(InsertCube node, Void context)
        {
            QualifiedObjectName targetCube = createQualifiedObjectName(session, node, node.getCubeName());
            Optional<CubeMetaStore> optionalCubeMetaStore = cubeManager.getMetaStore(STAR_TREE);
            CubeMetaStore cubeMetaStore = optionalCubeMetaStore.orElseThrow(() -> new PrestoException(StandardErrorCode.CUBE_ERROR, "Hetu metastore must be initialized."));
            CubeMetadata cubeMetadata = cubeMetaStore.getMetadataFromCubeName(targetCube.toString()).orElseThrow(() -> new PrestoException(StandardErrorCode.CUBE_ERROR, String.format("Cube not found '%s'", targetCube.toString())));
            Set<String> group = cubeMetadata.getGroup();
            ImmutableList.Builder<Identifier> builder = ImmutableList.builder();
            new IdentifierBuilderVisitor().process(node.getWhere(), builder);
            Set<String> whereColumns = builder.build()
                    .stream()
                    .map(Identifier::getValue)
                    .collect(Collectors.toCollection(() -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
            if (!group.containsAll(whereColumns)) {
                throw new IllegalArgumentException("All columns in where clause must be part Cube group.");
            }
            return buildCubeInsert(cubeMetadata, node, group);
        }

        private InsertCube buildCubeInsert(CubeMetadata cubeMetadata, InsertCube node, Set<String> cubeGroup)
        {
            Expression newDataPredicate = node.getWhere();
            QualifiedObjectName originalTableName = QualifiedObjectName.valueOf(cubeMetadata.getOriginalTableName());
            List<Identifier> insertColumns = new ArrayList<>();
            QualifiedName sourceTable = QualifiedName.of(originalTableName.getCatalogName(), originalTableName.getSchemaName(), originalTableName.getObjectName());
            List<SelectItem> selectItems = new ArrayList<>();
            cubeMetadata.getAggregations().forEach(aggColumn -> {
                AggregationSignature aggregationSignature = cubeMetadata.getAggregationSignature(aggColumn).get();
                FunctionCall aggFunction = new FunctionCall(
                        Optional.empty(),
                        QualifiedName.of(aggregationSignature.getFunction()),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        aggregationSignature.isDistinct(),
                        aggregationSignature.getDimension().equals("*") ?
                                Collections.emptyList() : Lists.newArrayList(new Identifier(aggregationSignature.getDimension())));
                insertColumns.add(new Identifier(aggColumn));
                selectItems.add(new SingleColumn(aggFunction));
            });
            cubeMetadata.getDimensions().forEach(dimension -> {
                Identifier identifier = new Identifier(dimension);
                if (cubeGroup.contains(dimension)) {
                    selectItems.add(new SingleColumn(identifier));
                }
                else {
                    selectItems.add(new SingleColumn(new NullLiteral(), identifier));
                }
                insertColumns.add(new Identifier(dimension));
            });
            List<List<Expression>> groupingSets = new ArrayList<>();
            groupingSets.add(cubeGroup
                    .stream()
                    .map(Identifier::new)
                    .collect(Collectors.toList()));
            GroupBy groupBy = new GroupBy(false, Lists.newArrayList(new GroupingSets(groupingSets)));
            QuerySpecification selectQuery = new QuerySpecification(
                    new Select(false, selectItems),
                    Optional.of(new Table(sourceTable)),
                    Optional.of(newDataPredicate),
                    Optional.of(groupBy),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            Query query = new Query(Optional.empty(), selectQuery, Optional.empty(), Optional.empty(), Optional.empty());
            if (node.getLocation().isPresent()) {
                return new InsertCube(node.getLocation().get(), node.getCubeName(), node.getWhere(), insertColumns, node.isOverwrite(), query);
            }
            else {
                return new InsertCube(node.getCubeName(), node.getWhere(), insertColumns, node.isOverwrite(), query);
            }
        }

        private static class IdentifierBuilderVisitor
                extends DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<Identifier>>
        {
            @Override
            protected Void visitIdentifier(Identifier node, ImmutableList.Builder<Identifier> builder)
            {
                builder.add(node);
                return null;
            }
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
