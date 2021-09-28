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

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.cube.CubeManager;
import io.prestosql.execution.SplitCacheMap;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.ExpressionDomainTranslator;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.rule.SimplifyExpressions;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.Cache;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.Values;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.sql.QueryUtil.aliased;
import static io.prestosql.sql.QueryUtil.aliasedName;
import static io.prestosql.sql.QueryUtil.selectList;
import static io.prestosql.sql.QueryUtil.simpleQuery;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_COLUMN;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_OPERATOR;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_PREDICATE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_TABLE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_CACHE;
import static java.util.Objects.requireNonNull;

final class CacheTableRewrite
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
        return (Statement) new Visitor(session, parser, metadata, node, queryExplainer, parameters, accessControl, warningCollector).process(node, null);
    }

    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final SqlParser sqlParser;
        private final Statement node;
        private final List<Expression> parameters;
        private final Optional<QueryExplainer> queryExplainer;
        private final AccessControl accessControl;
        private final WarningCollector warningCollector;

        public Visitor(
                Session session,
                SqlParser sqlParser,
                Metadata metadata,
                Statement node,
                Optional<QueryExplainer> queryExplainer,
                List<Expression> parameters,
                AccessControl accessControl,
                WarningCollector warningCollector)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.node = requireNonNull(node, "node is null");
            this.parameters = requireNonNull(parameters, "parameters is null");
            this.queryExplainer = queryExplainer;
            this.accessControl = accessControl;
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        protected Node visitCache(Cache cache, Void context)
        {
            if (!PropertyService.getBooleanProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED)) {
                throw new PrestoException(GENERIC_USER_ERROR, "Cache table feature is not enabled");
            }
            SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();
            QualifiedObjectName qualifiedTableName = createQualifiedObjectName(session, cache, cache.getTableName());
            // Always expand tableName to full qualified name
            QualifiedName tableName = QualifiedName.of(qualifiedTableName.getCatalogName(), qualifiedTableName.getSchemaName(), qualifiedTableName.getObjectName());
            Expression predicate = cache.getWhere().get();

            if (!metadata.getView(session, qualifiedTableName).isPresent() &&
                    !metadata.getTableHandle(session, qualifiedTableName).isPresent()) {
                throw new SemanticException(MISSING_CACHE, cache, "Table '%s' does not exist", qualifiedTableName.toString());
            }

            ConnectorTableHandle tableHandle = metadata.getTableHandle(session, qualifiedTableName).get().getConnectorHandle();
            if (!tableHandle.isTableCacheable()) {
                throw new SemanticException(INVALID_TABLE, cache, "Table '%s' cannot be cached", qualifiedTableName.toString());
            }

            TableMetadata tableMetadata = metadata.getTableMetadata(session, metadata.getTableHandle(session, qualifiedTableName).get());

            TupleDomain<ColumnMetadata> columnMetadataTupleDomain = translateToTupleDomain(tableMetadata, predicate);
            splitCacheMap.addCache(tableName, columnMetadataTupleDomain, predicate.toString());

            // Construct a response to indicate success
            return simpleQuery(
                    selectList(
                            aliasedName("result", "Result")),
                    aliased(
                            new Values(ImmutableList.of(new StringLiteral("OK"))),
                            "Cache Result",
                            ImmutableList.of("result")));
            // return a no-op value node, see SHOW CATALOGS flow, return "success"
        }

        @Override
        protected Node visitQuery(Query node, Void context)
        {
            return super.visitQuery(node, context);
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        private TupleDomain<ColumnMetadata> translateToTupleDomain(TableMetadata tableMetadata, Expression whereClause)
        {
            // convert expression into a tuple domain
            // rewrite all Identifiers into SymbolReference
            TypeProvider types = null;
            Expression rewrittenPredicate = null;
            LiteralEncoder literalEncoder = new LiteralEncoder(metadata);
            Map<Symbol, Type> symbolMapping = null;
            ColumnMetadata columnMetadata = null;
            Identifier identifier = null;
            Expression value = null;

            if (whereClause instanceof ComparisonExpression) {
                ComparisonExpression predicate = (ComparisonExpression) whereClause;
                identifier = (Identifier) ((predicate.getLeft() instanceof Identifier) ? predicate.getLeft() : predicate.getRight());
                value = (predicate.getLeft() instanceof Identifier) ? predicate.getRight() : predicate.getLeft();
                rewrittenPredicate = new ComparisonExpression(predicate.getOperator(), new SymbolReference(identifier.getValue()), value);
            }
            else if (whereClause instanceof BetweenPredicate) {
                BetweenPredicate predicate = (BetweenPredicate) whereClause;
                identifier = (Identifier) predicate.getValue();
                rewrittenPredicate = new BetweenPredicate(new SymbolReference(predicate.getValue().toString()), predicate.getMin(), predicate.getMax());
            }
            else if (whereClause instanceof IsNullPredicate) {
                IsNullPredicate predicate = (IsNullPredicate) whereClause;
                identifier = (Identifier) predicate.getValue();
                rewrittenPredicate = new IsNullPredicate(new SymbolReference(predicate.getValue().toString()));
            }
            else if (whereClause instanceof IsNotNullPredicate) {
                IsNotNullPredicate predicate = (IsNotNullPredicate) whereClause;
                identifier = (Identifier) predicate.getValue();
                rewrittenPredicate = new IsNotNullPredicate(new SymbolReference(predicate.getValue().toString()));
            }
            else if (whereClause instanceof LogicalBinaryExpression) {
                LogicalBinaryExpression predicate = (LogicalBinaryExpression) whereClause;
                Expression leftExpression = predicate.getLeft();
                Expression rightExpression = predicate.getRight();
                TupleDomain<ColumnMetadata> leftDomain = translateToTupleDomain(tableMetadata, leftExpression);
                TupleDomain<ColumnMetadata> rightDomain = translateToTupleDomain(tableMetadata, rightExpression);
                switch (predicate.getOperator()) {
                    case AND:
                        return leftDomain.intersect(rightDomain);
                    case OR:
                        throw new SemanticException(INVALID_OPERATOR, node, "%s operator is not supported", predicate.getOperator().toString());
                }
            }
            else if (whereClause instanceof LikePredicate) {
                throw new SemanticException(INVALID_PREDICATE, node, "LIKE predicate is not supported.");
            }
            else {
                throw new PrestoException(GENERIC_USER_ERROR, "Cache table predicate is invalid");
            }

            // create a Symbol to Type mapping  entry for the converted Identifier
            symbolMapping = Collections.singletonMap(new Symbol(identifier.getValue()), tableMetadata.getColumn(identifier.getValue()).getType());
            columnMetadata = tableMetadata.getColumn(identifier.getValue());
            // verify if column is partitioned
            // Hive columns contain extra info on whether or not a column is a partition key
            // see: io.prestosql.plugin.hive.HiveUtil.columnExtraInfo
            if (columnMetadata.getExtraInfo() == null) {
                throw new SemanticException(INVALID_COLUMN, node, "Column '%s' is not cacheable", columnMetadata.getName());
            }

            // create a Type entry for the converted Identifier
            types = TypeProvider.copyOf(symbolMapping);
            // Use SimplifyExpressions class to rewrite the replacement predicate into a new expression
            Expression rewritten = SimplifyExpressions.rewrite(rewrittenPredicate,
                    session,
                    new PlanSymbolAllocator(symbolMapping),
                    metadata,
                    literalEncoder,
                    new TypeAnalyzer(sqlParser, metadata));

            // Extract TupleDomain from the new expression
            TupleDomain<Symbol> tupleDomain = ExpressionDomainTranslator.fromPredicate(metadata, session, rewritten, types).getTupleDomain();
            HashMap<ColumnMetadata, Domain> columnDomainMap = new HashMap<>();

            ColumnMetadata finalColumnMetadata = columnMetadata;
            tupleDomain.getDomains().get().forEach((symbol, domain) ->
                    columnDomainMap.put(finalColumnMetadata, domain));
            return TupleDomain.withColumnDomains(columnDomainMap);
        }
    }
}
