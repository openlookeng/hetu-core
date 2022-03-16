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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.hetu.core.spi.cube.CubeFilter;
import io.hetu.core.spi.cube.CubeMetadata;
import io.hetu.core.spi.cube.CubeStatus;
import io.hetu.core.spi.cube.aggregator.AggregationSignature;
import io.hetu.core.spi.cube.io.CubeMetaStore;
import io.prestosql.Session;
import io.prestosql.connector.DataCenterUtility;
import io.prestosql.cube.CubeManager;
import io.prestosql.execution.SplitCacheMap;
import io.prestosql.execution.TableCacheInfo;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.SessionPropertyManager.SessionPropertyValue;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.function.SqlFunction;
import io.prestosql.spi.function.SqlInvokedFunction;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.ArrayConstructor;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.ColumnDefinition;
import io.prestosql.sql.tree.CreateCube;
import io.prestosql.sql.tree.CreateFunction;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Explain;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExternalBodyReference;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.FunctionProperty;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.Property;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.RefreshMetadataCache;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.RoutineCharacteristics;
import io.prestosql.sql.tree.ShowCache;
import io.prestosql.sql.tree.ShowCatalogs;
import io.prestosql.sql.tree.ShowColumns;
import io.prestosql.sql.tree.ShowCreate;
import io.prestosql.sql.tree.ShowCubes;
import io.prestosql.sql.tree.ShowExternalFunction;
import io.prestosql.sql.tree.ShowFunctions;
import io.prestosql.sql.tree.ShowGrants;
import io.prestosql.sql.tree.ShowIndex;
import io.prestosql.sql.tree.ShowRoleGrants;
import io.prestosql.sql.tree.ShowRoles;
import io.prestosql.sql.tree.ShowSchemas;
import io.prestosql.sql.tree.ShowSession;
import io.prestosql.sql.tree.ShowTables;
import io.prestosql.sql.tree.ShowViews;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.SqlParameterDeclaration;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.TableElement;
import io.prestosql.sql.tree.Values;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_COLUMNS;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_ENABLED_ROLES;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_ROLES;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_SCHEMATA;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_TABLES;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_TABLE_PRIVILEGES;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.TABLE_VIEWS;
import static io.prestosql.cube.CubeManager.STAR_TREE;
import static io.prestosql.metadata.FunctionAndTypeManager.qualifyObjectName;
import static io.prestosql.metadata.MetadataListing.listCatalogs;
import static io.prestosql.metadata.MetadataListing.listSchemas;
import static io.prestosql.metadata.MetadataUtil.createCatalogSchemaName;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.metadata.MetadataUtil.toCatalogSchemaTableName;
import static io.prestosql.spi.HetuConstant.INDEX_OK;
import static io.prestosql.spi.HetuConstant.INDEX_OUT_OF_SYNC;
import static io.prestosql.spi.HetuConstant.INDEX_TABLE_DELETED;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.prestosql.spi.connector.CatalogSchemaName.DEFAULT_NAMESPACE;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.ParsingUtil.createParsingOptions;
import static io.prestosql.sql.QueryUtil.aliased;
import static io.prestosql.sql.QueryUtil.aliasedName;
import static io.prestosql.sql.QueryUtil.aliasedNullToEmpty;
import static io.prestosql.sql.QueryUtil.ascending;
import static io.prestosql.sql.QueryUtil.descending;
import static io.prestosql.sql.QueryUtil.equal;
import static io.prestosql.sql.QueryUtil.functionCall;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.sql.QueryUtil.logicalAnd;
import static io.prestosql.sql.QueryUtil.ordering;
import static io.prestosql.sql.QueryUtil.row;
import static io.prestosql.sql.QueryUtil.selectAll;
import static io.prestosql.sql.QueryUtil.selectList;
import static io.prestosql.sql.QueryUtil.simpleQuery;
import static io.prestosql.sql.QueryUtil.singleValueQuery;
import static io.prestosql.sql.QueryUtil.table;
import static io.prestosql.sql.SqlFormatter.formatSql;
import static io.prestosql.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_CACHE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_CUBE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.analyzer.SemanticErrorCode.VIEW_PARSE_ERROR;
import static io.prestosql.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ShowCreate.Type.CUBE;
import static io.prestosql.sql.tree.ShowCreate.Type.TABLE;
import static io.prestosql.sql.tree.ShowCreate.Type.VIEW;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

final class ShowQueriesRewrite
        implements StatementRewrite.Rewrite
{
    private static final int COL_MAX_LENGTH = 70;

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
        return (Statement) new Visitor(cubeManager, metadata, parser, session, parameters, accessControl, warningCollector, heuristicIndexerManager).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final CubeManager cubeManager;
        private final Metadata metadata;
        private final Session session;
        private final SqlParser sqlParser;
        private final List<Expression> parameters;
        private final AccessControl accessControl;
        private final HeuristicIndexerManager heuristicIndexerManager;
        private final WarningCollector warningCollector;

        public Visitor(CubeManager cubeManager, Metadata metadata, SqlParser sqlParser, Session session, List<Expression> parameters, AccessControl accessControl, WarningCollector warningCollector, HeuristicIndexerManager heuristicIndexerManager)
        {
            //TODO: Replace with NoOpCubeManager. Here CubeManager can be null
            this.cubeManager = cubeManager;
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.session = requireNonNull(session, "session is null");
            this.parameters = requireNonNull(parameters, "parameters is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        protected Node visitExplain(Explain node, Void context)
        {
            Statement statement = (Statement) process(node.getStatement(), null);
            return new Explain(
                    node.getLocation().get(),
                    node.isAnalyze(),
                    node.isVerbose(),
                    statement,
                    node.getOptions());
        }

        @Override
        protected Node visitShowTables(ShowTables showTables, Void context)
        {
            if (metadata instanceof MetadataManager) {
                MetadataManager metadataManager = (MetadataManager) metadata;
                if (metadataManager.getDataCenterConnectorManager() != null) {
                    metadataManager.getDataCenterConnectorManager().loadAllDCCatalogs();
                }
            }
            CatalogSchemaName schema = createCatalogSchemaName(session, showTables, showTables.getSchema());

            accessControl.checkCanShowTablesMetadata(session.getRequiredTransactionId(), session.getIdentity(), schema);

            if (!metadata.catalogExists(session, schema.getCatalogName())) {
                throw new SemanticException(MISSING_CATALOG, showTables, "Catalog '%s' does not exist", schema.getCatalogName());
            }

            if (!metadata.schemaExists(session, schema)) {
                throw new SemanticException(MISSING_SCHEMA, showTables, "Schema '%s' does not exist", schema.getSchemaName());
            }

            Expression predicate = equal(identifier("table_schema"), new StringLiteral(schema.getSchemaName()));

            Optional<String> likePattern = showTables.getLikePattern();
            if (likePattern.isPresent()) {
                Expression likePredicate = new LikePredicate(
                        identifier("table_name"),
                        new StringLiteral(likePattern.get()),
                        showTables.getEscape().map(StringLiteral::new));
                predicate = logicalAnd(predicate, likePredicate);
            }

            return simpleQuery(
                    selectList(aliasedName("table_name", "Table")),
                    from(schema.getCatalogName(), TABLE_TABLES),
                    predicate,
                    ordering(ascending("table_name")));
        }

        @Override
        protected Node visitShowCubes(ShowCubes node, Void context)
        {
            ImmutableList.Builder<Expression> rows = ImmutableList.builder();
            CubeMetaStore cubeMetaStore = this.cubeManager.getMetaStore(STAR_TREE).orElseThrow(() -> new RuntimeException("HetuMetastore is not initialized"));
            List<CubeMetadata> cubeMetadataList;
            if (!node.getTableName().isPresent()) {
                cubeMetadataList = cubeMetaStore.getAllCubes();
            }
            else {
                QualifiedObjectName qualifiedTableName = createQualifiedObjectName(session, node, node.getTableName().get());
                Optional<TableHandle> tableHandle = metadata.getTableHandle(session, qualifiedTableName);
                if (!tableHandle.isPresent()) {
                    throw new SemanticException(MISSING_TABLE, node, "Table %s does not exist", qualifiedTableName.toString());
                }
                cubeMetadataList = cubeMetaStore.getMetadataList(qualifiedTableName.toString());
            }
            Map<String, String> cubeStatusMap = new HashMap<>();
            cubeMetadataList.forEach(cubeMetadata -> {
                QualifiedObjectName qualifiedTableName = QualifiedObjectName.valueOf(cubeMetadata.getSourceTableName());
                Map<QualifiedObjectName, Long> tableLastModifiedTimeMap = new HashMap<>();
                long tableLastModifiedTime = tableLastModifiedTimeMap.computeIfAbsent(qualifiedTableName, ignored -> {
                    Optional<TableHandle> tableHandle = metadata.getTableHandle(session, qualifiedTableName);
                    if (!tableHandle.isPresent()) {
                        return -1L;
                    }
                    LongSupplier lastModifiedTimeSupplier = metadata.getTableLastModifiedTimeSupplier(session, tableHandle.get());
                    return lastModifiedTimeSupplier == null ? -1L : lastModifiedTimeSupplier.getAsLong();
                });
                CubeStatus status = cubeMetadata.getCubeStatus();
                if (status == CubeStatus.INACTIVE) {
                    cubeStatusMap.put(cubeMetadata.getCubeName(), "Inactive");
                }
                else if (tableLastModifiedTime == -1L) {
                    // The table handle isn't present, or we got an error while trying to retrieve last modified time
                    cubeStatusMap.put(cubeMetadata.getCubeName(), "Invalid");
                }
                else {
                    cubeStatusMap.put(cubeMetadata.getCubeName(), tableLastModifiedTime > cubeMetadata.getSourceTableLastUpdatedTime() ? "Expired" : "Active");
                }
            });
            rows.add(row(
                    new StringLiteral(""),
                    new StringLiteral(""),
                    new StringLiteral(""),
                    new StringLiteral(""),
                    new StringLiteral(""),
                    new StringLiteral(""),
                    FALSE_LITERAL));
            cubeMetadataList.forEach(cubeMetadata -> {
                CubeFilter cubeFilter = cubeMetadata.getCubeFilter();
                String cubeFilterString = "";
                if (cubeFilter != null) {
                    if (cubeFilter.getSourceTablePredicate() != null) {
                        cubeFilterString = cubeFilter.getSourceTablePredicate() + Optional.ofNullable(cubeFilter.getCubePredicate())
                                .map(predicate -> " AND " + predicate)
                                .orElse("");
                    }
                    else {
                        cubeFilterString = cubeFilter.getCubePredicate();
                    }
                }
                rows.add(row(
                        new StringLiteral(cubeMetadata.getSourceTableName()),
                        new StringLiteral(cubeMetadata.getCubeName()),
                        new StringLiteral(cubeStatusMap.get(cubeMetadata.getCubeName())),
                        new StringLiteral(String.join(",", cubeMetadata.getDimensions())),
                        new StringLiteral(cubeMetadata.getAggregationSignatures().stream().map(AggregationSignature::toString).collect(Collectors.joining(","))),
                        new StringLiteral(cubeFilterString),
                        TRUE_LITERAL));
            });

            ImmutableList<Expression> expressions = rows.build();
            return simpleQuery(
                    selectList(
                            aliasedName("table_name", "Table Name"),
                            aliasedName("cube_name", "Cube Name"),
                            aliasedName("cube_status", "Status"),
                            aliasedName("dimensions", "Dimensions"),
                            aliasedName("aggregations", "Aggregations"),
                            aliasedName("cube_predicate", "Cube Predicate")),
                    aliased(
                            new Values(expressions),
                            "Cube Result",
                            ImmutableList.of("table_name", "cube_name", "cube_status", "dimensions", "aggregations", "cube_predicate", "include")),
                    identifier("include"),
                    ordering(ascending("table_name"), ascending("cube_name")));
        }

        @Override
        protected Node visitShowGrants(ShowGrants showGrants, Void context)
        {
            String catalogName = session.getCatalog().orElse(null);
            Optional<Expression> predicate = Optional.empty();

            Optional<QualifiedName> tableName = showGrants.getTableName();
            if (tableName.isPresent()) {
                QualifiedObjectName qualifiedTableName = createQualifiedObjectName(session, showGrants, tableName.get());

                if (!metadata.getView(session, qualifiedTableName).isPresent() &&
                        !metadata.getTableHandle(session, qualifiedTableName).isPresent()) {
                    throw new SemanticException(MISSING_TABLE, showGrants, "Table '%s' does not exist", tableName);
                }

                catalogName = qualifiedTableName.getCatalogName();

                accessControl.checkCanShowTablesMetadata(
                        session.getRequiredTransactionId(),
                        session.getIdentity(),
                        new CatalogSchemaName(catalogName, qualifiedTableName.getSchemaName()));

                predicate = Optional.of(combineConjuncts(
                        equal(identifier("table_schema"), new StringLiteral(qualifiedTableName.getSchemaName())),
                        equal(identifier("table_name"), new StringLiteral(qualifiedTableName.getObjectName()))));
            }
            else {
                if (catalogName == null) {
                    throw new SemanticException(CATALOG_NOT_SPECIFIED, showGrants, "Catalog must be specified when session catalog is not set");
                }

                Set<String> allowedSchemas = listSchemas(session, metadata, accessControl, catalogName);
                for (String schema : allowedSchemas) {
                    accessControl.checkCanShowTablesMetadata(session.getRequiredTransactionId(), session.getIdentity(), new CatalogSchemaName(catalogName, schema));
                }
            }

            return simpleQuery(
                    selectList(
                            aliasedName("grantor", "Grantor"),
                            aliasedName("grantor_type", "Grantor Type"),
                            aliasedName("grantee", "Grantee"),
                            aliasedName("grantee_type", "Grantee Type"),
                            aliasedName("table_catalog", "Catalog"),
                            aliasedName("table_schema", "Schema"),
                            aliasedName("table_name", "Table"),
                            aliasedName("privilege_type", "Privilege"),
                            aliasedName("is_grantable", "Grantable"),
                            aliasedName("with_hierarchy", "With Hierarchy")),
                    from(catalogName, TABLE_TABLE_PRIVILEGES),
                    predicate,
                    Optional.empty());
        }

        @Override
        protected Node visitShowRoles(ShowRoles node, Void context)
        {
            if (!node.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
                throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(c -> c.getValue().toLowerCase(ENGLISH)).orElseGet(() -> session.getCatalog().get());

            if (node.isCurrent()) {
                accessControl.checkCanShowCurrentRoles(session.getRequiredTransactionId(), session.getIdentity(), catalog);
                return simpleQuery(
                        selectList(aliasedName("role_name", "Role")),
                        from(catalog, TABLE_ENABLED_ROLES));
            }
            else {
                accessControl.checkCanShowRoles(session.getRequiredTransactionId(), session.getIdentity(), catalog);
                return simpleQuery(
                        selectList(aliasedName("role_name", "Role")),
                        from(catalog, TABLE_ROLES));
            }
        }

        @Override
        protected Node visitShowRoleGrants(ShowRoleGrants node, Void context)
        {
            if (!node.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
                throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(c -> c.getValue().toLowerCase(ENGLISH)).orElseGet(() -> session.getCatalog().get());
            PrestoPrincipal principal = new PrestoPrincipal(PrincipalType.USER, session.getUser());

            accessControl.checkCanShowRoleGrants(session.getRequiredTransactionId(), session.getIdentity(), catalog);
            List<Expression> rows = metadata.listRoleGrants(session, catalog, principal).stream()
                    .map(roleGrant -> row(new StringLiteral(roleGrant.getRoleName())))
                    .collect(toList());

            return simpleQuery(
                    selectList(new AllColumns()),
                    aliased(new Values(rows), "role_grants", ImmutableList.of("Role Grants")),
                    ordering(ascending("Role Grants")));
        }

        @Override
        protected Node visitShowSchemas(ShowSchemas node, Void context)
        {
            if (!node.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
                throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(Identifier::getValue).orElseGet(() -> session.getCatalog().get());
            // changes for show schemas
            DataCenterUtility.loadDCCatalogForQueryFlow(session, metadata, catalog);
            accessControl.checkCanShowSchemas(session.getRequiredTransactionId(), session.getIdentity(), catalog);

            Optional<Expression> predicate = Optional.empty();
            Optional<String> likePattern = node.getLikePattern();
            if (likePattern.isPresent()) {
                predicate = Optional.of(new LikePredicate(
                        identifier("schema_name"),
                        new StringLiteral(likePattern.get()),
                        node.getEscape().map(StringLiteral::new)));
            }

            return simpleQuery(
                    selectList(aliasedName("schema_name", "Schema")),
                    from(catalog, TABLE_SCHEMATA),
                    predicate,
                    Optional.of(ordering(ascending("schema_name"))));
        }

        @Override
        protected Node visitShowCatalogs(ShowCatalogs node, Void context)
        {
            // Changes for loading all the remote presto sub catalogs
            DataCenterUtility.loadDCCatalogsForShowQueries(metadata, session);
            List<Expression> rows = listCatalogs(session, metadata, accessControl).keySet().stream()
                    .map(name -> row(new StringLiteral(name)))
                    .collect(toList());

            Optional<Expression> predicate = Optional.empty();
            Optional<String> likePattern = node.getLikePattern();
            if (likePattern.isPresent()) {
                predicate = Optional.of(new LikePredicate(identifier("Catalog"), new StringLiteral(likePattern.get()), Optional.empty()));
            }

            return simpleQuery(
                    selectList(new AllColumns()),
                    aliased(new Values(rows), "catalogs", ImmutableList.of("Catalog")),
                    predicate,
                    Optional.of(ordering(ascending("Catalog"))));
        }

        @Override
        protected Node visitShowColumns(ShowColumns showColumns, Void context)
        {
            QualifiedObjectName tableName = createQualifiedObjectName(session, showColumns, showColumns.getTable());

            if (!metadata.getView(session, tableName).isPresent() &&
                    !metadata.getTableHandle(session, tableName).isPresent()) {
                throw new SemanticException(MISSING_TABLE, showColumns, "Table '%s' does not exist", tableName);
            }

            accessControl.checkCanShowColumnsMetadata(session.getRequiredTransactionId(), session.getIdentity(), toCatalogSchemaTableName(tableName));

            return simpleQuery(
                    selectList(
                            aliasedName("column_name", "Column"),
                            aliasedName("data_type", "Type"),
                            aliasedNullToEmpty("extra_info", "Extra"),
                            aliasedNullToEmpty("comment", "Comment")),
                    from(tableName.getCatalogName(), TABLE_COLUMNS),
                    logicalAnd(
                            equal(identifier("table_schema"), new StringLiteral(tableName.getSchemaName())),
                            equal(identifier("table_name"), new StringLiteral(tableName.getObjectName()))),
                    ordering(ascending("ordinal_position")));
        }

        private static <T> Expression getExpression(PropertyMetadata<T> property, Object value)
                throws PrestoException
        {
            return toExpression(property.encode(property.getJavaType().cast(value)));
        }

        private static Expression toExpression(Object value)
                throws PrestoException
        {
            if (value instanceof String) {
                return new StringLiteral(value.toString());
            }

            if (value instanceof Boolean) {
                return new BooleanLiteral(value.toString());
            }

            if (value instanceof Long || value instanceof Integer) {
                return new LongLiteral(value.toString());
            }

            if (value instanceof Double) {
                return new DoubleLiteral(value.toString());
            }

            if (value instanceof List) {
                List<?> list = (List<?>) value;
                return new ArrayConstructor(list.stream()
                        .map(Visitor::toExpression)
                        .collect(toList()));
            }

            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Failed to convert object of type %s to expression: %s", value.getClass().getName(), value));
        }

        @Override
        protected Node visitShowCreate(ShowCreate node, Void context)
        {
            QualifiedObjectName objectName = createQualifiedObjectName(session, node, node.getName());
            Optional<ConnectorViewDefinition> viewDefinition = metadata.getView(session, objectName);

            if (node.getType() == VIEW) {
                if (!viewDefinition.isPresent()) {
                    if (metadata.getTableHandle(session, objectName).isPresent()) {
                        throw new SemanticException(NOT_SUPPORTED, node, "Relation '%s' is a table, not a view", objectName);
                    }
                    throw new SemanticException(MISSING_TABLE, node, "View '%s' does not exist", objectName);
                }

                Query query = parseView(viewDefinition.get().getOriginalSql(), objectName, node);
                List<Identifier> parts = Lists.reverse(node.getName().getOriginalParts());
                Identifier tableName = parts.get(0);
                Identifier schemaName = (parts.size() > 1) ? parts.get(1) : new Identifier(objectName.getSchemaName());
                Identifier catalogName = (parts.size() > 2) ? parts.get(2) : new Identifier(objectName.getCatalogName());
                String sql = formatSql(new CreateView(QualifiedName.of(ImmutableList.of(catalogName, schemaName, tableName)), query, false, Optional.empty()), Optional.of(parameters)).trim();
                return singleValueQuery("Create View", sql);
            }

            if (node.getType() == TABLE) {
                if (viewDefinition.isPresent()) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Relation '%s' is a view, not a table", objectName);
                }

                Optional<TableHandle> tableHandle = metadata.getTableHandle(session, objectName);
                if (!tableHandle.isPresent()) {
                    throw new SemanticException(MISSING_TABLE, node, "Table '%s' does not exist", objectName);
                }

                ConnectorTableMetadata connectorTableMetadata = metadata.getTableMetadata(session, tableHandle.get()).getMetadata();

                Map<String, PropertyMetadata<?>> allColumnProperties = metadata.getColumnPropertyManager().getAllProperties().get(tableHandle.get().getCatalogName());

                List<TableElement> columns = connectorTableMetadata.getColumns().stream()
                        .filter(column -> !column.isHidden())
                        .map(column -> {
                            List<Property> propertyNodes = buildProperties(objectName, Optional.of(column.getName()), INVALID_COLUMN_PROPERTY, column.getProperties(), allColumnProperties);
                            return new ColumnDefinition(new Identifier(column.getName()), column.getType().getDisplayName(), column.isNullable(), propertyNodes, Optional.ofNullable(column.getComment()));
                        })
                        .collect(toImmutableList());

                Map<String, Object> properties = connectorTableMetadata.getProperties();
                Map<String, PropertyMetadata<?>> allTableProperties = metadata.getTablePropertyManager().getAllProperties().get(tableHandle.get().getCatalogName());
                List<Property> propertyNodes = buildProperties(objectName, Optional.empty(), INVALID_TABLE_PROPERTY, properties, allTableProperties);

                CreateTable createTable = new CreateTable(
                        QualifiedName.of(objectName.getCatalogName(), objectName.getSchemaName(), objectName.getObjectName()),
                        columns,
                        false,
                        propertyNodes,
                        connectorTableMetadata.getComment());
                return singleValueQuery("Create Table", formatSql(createTable, Optional.of(parameters)).trim());
            }

            if (node.getType() == CUBE) {
                SqlParser parser = new SqlParser();
                ImmutableList.Builder<Expression> rows = ImmutableList.builder();
                CubeMetaStore cubeMetaStore = this.cubeManager.getMetaStore(STAR_TREE).orElseThrow(() -> new RuntimeException("HetuMetastore is not initialized"));
                Optional<TableHandle> tableHandle = metadata.getTableHandle(session, objectName);
                if (!tableHandle.isPresent()) {
                    throw new SemanticException(MISSING_CUBE, node, "CUBE '%s' does not exist", objectName);
                }
                ConnectorTableMetadata connectorTableMetadata = metadata.getTableMetadata(session, tableHandle.get()).getMetadata();
                QualifiedObjectName cubeTableName = createQualifiedObjectName(session, node, node.getName());
                Optional<CubeMetadata> matchedCube = cubeMetaStore.getMetadataFromCubeName(cubeTableName.toString());
                CubeMetadata cubeMetadata = matchedCube.orElseThrow(() -> new SemanticException(MISSING_CUBE, node, "Cube '%s' does not exist", objectName));
                QualifiedObjectName qualifiedTableName = QualifiedObjectName.valueOf(cubeMetadata.getSourceTableName());
                String aggregation = cubeMetadata.getAggregationSignatures().stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(", "));
                String predicate = Optional.ofNullable(cubeMetadata.getCubeFilter())
                        .map(CubeFilter::getCubePredicate)
                        .orElse(null);
                String dimension = "";
                if (cubeMetadata.getCubeFilter() != null && cubeMetadata.getCubeFilter().getSourceTablePredicate() != null) {
                    Set<Identifier> sourceFilterPredicateColumns = ExpressionUtils.getIdentifiers(sqlParser.createExpression(cubeMetadata.getCubeFilter().getSourceTablePredicate(), new ParsingOptions()));
                    final List<String> filterColumns = sourceFilterPredicateColumns.stream().map(Identifier::getValue).map(String::valueOf).collect(Collectors.toList());
                    dimension = cubeMetadata.getGroup().stream()
                            .filter(x -> !filterColumns.contains(x))
                            .map(String::valueOf)
                            .collect(Collectors.joining(", "));
                }
                else {
                    dimension = cubeMetadata.getGroup().stream()
                            .map(String::valueOf)
                            .collect(Collectors.joining(", "));
                }
                String allPartitions = "";
                if (connectorTableMetadata.getProperties().containsKey("partitioned_by")) {
                    allPartitions = Arrays.stream(connectorTableMetadata.getProperties().get("partitioned_by").toString()
                            .substring(1, connectorTableMetadata.getProperties().get("partitioned_by").toString().length() - 1).split(","))
                            .map(String::valueOf)
                            .map(i -> "'" + i.trim() + "'")
                            .collect(Collectors.joining(", "));
                }
                StringBuilder query = new StringBuilder();
                query.append("CREATE CUBE ").append(cubeTableName).append(" ON ").append(qualifiedTableName).append(" WITH (AGGREGATIONS=(").append(aggregation).append("), GROUP=(").append(dimension).append(")");
                if (connectorTableMetadata.getProperties().containsKey("format")) {
                    query.append(", format='").append(Optional.ofNullable(connectorTableMetadata.getProperties().get("format"))
                            .map(String::valueOf)
                            .orElse(null)).append("'");
                }
                if (!allPartitions.equals("")) {
                    query.append(", partitioned_by=ARRAY[").append(allPartitions).append("]");
                }
                if (cubeMetadata.getCubeFilter() != null && cubeMetadata.getCubeFilter().getSourceTablePredicate() != null) {
                    query.append(", FILTER=(").append(cubeMetadata.getCubeFilter().getSourceTablePredicate()).append(")");
                }
                query.append(")");
                if (predicate != null) {
                    query.append(" WHERE ").append(predicate);
                }
                CreateCube createCube = (CreateCube) parser.createStatement(query.toString(), new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE));
                QualifiedName cubeName = createCube.getCubeName();
                Optional<Expression> cubePredicate = createCube.getWhere();
                QualifiedName sourceTableName = createCube.getSourceTableName();
                Set<FunctionCall> aggregations = createCube.getAggregations();
                List<Identifier> groupingSet = createCube.getGroupingSet();
                List<Property> properties = createCube.getProperties();
                boolean notExists = createCube.isNotExists();
                CreateCube modifiedCreateCube = new CreateCube(cubeName, sourceTableName, groupingSet, aggregations, notExists, properties, cubePredicate, createCube.getSourceFilter().orElse(null));
                return singleValueQuery("Create Cube", formatSql(modifiedCreateCube, Optional.of(parameters)).trim());
            }
            throw new UnsupportedOperationException("SHOW CREATE only supported for tables, views and cubes");
        }

        private List<Property> buildProperties(
                Object objectName,
                Optional<String> columnName,
                StandardErrorCode errorCode,
                Map<String, Object> properties,
                Map<String, PropertyMetadata<?>> allProperties)
        {
            if (properties.isEmpty()) {
                return Collections.emptyList();
            }

            ImmutableSortedMap.Builder<String, Expression> sqlProperties = ImmutableSortedMap.naturalOrder();

            for (Map.Entry<String, Object> propertyEntry : properties.entrySet()) {
                String propertyName = propertyEntry.getKey();
                Object value = propertyEntry.getValue();
                if (value == null) {
                    throw new PrestoException(errorCode, format("Property %s for %s cannot have a null value", propertyName, toQualifiedName(objectName, columnName)));
                }

                PropertyMetadata<?> property = allProperties.get(propertyName);
                if (!Primitives.wrap(property.getJavaType()).isInstance(value)) {
                    throw new PrestoException(errorCode, format(
                            "Property %s for %s should have value of type %s, not %s",
                            propertyName,
                            toQualifiedName(objectName, columnName),
                            property.getJavaType().getName(),
                            value.getClass().getName()));
                }

                Expression sqlExpression = getExpression(property, value);
                sqlProperties.put(propertyName, sqlExpression);
            }

            return sqlProperties.build().entrySet().stream()
                    .map(entry -> new Property(new Identifier(entry.getKey()), entry.getValue()))
                    .collect(toImmutableList());
        }

        private static String toQualifiedName(Object objectName, Optional<String> columnName)
        {
            return columnName.map(s -> format("column %s of table %s", s, objectName))
                    .orElseGet(() -> "table " + objectName);
        }

        @Override
        protected Node visitShowFunctions(ShowFunctions node, Void context)
        {
            ImmutableList.Builder<Expression> rows = ImmutableList.builder();
            for (SqlFunction function : metadata.listFunctions(Optional.of(session))) {
                Signature signature = function.getSignature();
                boolean builtIn = signature.getName().getCatalogSchemaName().equals(DEFAULT_NAMESPACE);
                rows.add(row(
                        builtIn ? new StringLiteral(signature.getNameSuffix()) : new StringLiteral(signature.getName().toString()),
                        new StringLiteral(signature.getReturnType().toString()),
                        new StringLiteral(Joiner.on(", ").join(signature.getArgumentTypes())),
                        new StringLiteral(getFunctionType(function)),
                        function.isDeterministic() ? TRUE_LITERAL : FALSE_LITERAL,
                        new StringLiteral(nullToEmpty(function.getDescription())),
                        signature.isVariableArity() ? TRUE_LITERAL : FALSE_LITERAL,
                        builtIn ? TRUE_LITERAL : FALSE_LITERAL));
            }

            Map<String, String> columns = ImmutableMap.<String, String>builder()
                    .put("function_name", "Function")
                    .put("return_type", "Return Type")
                    .put("argument_types", "Argument Types")
                    .put("function_type", "Function Type")
                    .put("deterministic", "Deterministic")
                    .put("description", "Description")
                    .put("variable_arity", "Variable Arity")
                    .put("built_in", "Built In")
                    .build();

            return simpleQuery(
                    selectAll(columns.entrySet().stream()
                            .map(entry -> aliasedName(entry.getKey(), entry.getValue()))
                            .collect(toImmutableList())),
                    aliased(new Values(rows.build()), "functions", ImmutableList.copyOf(columns.keySet())),
                    node.getLikePattern().map(pattern -> new LikePredicate(
                            identifier("function_name"),
                            new StringLiteral(pattern),
                            node.getEscape().map(StringLiteral::new))),
                    Optional.of(ordering(
                            descending("built_in"),
                            new SortItem(
                                    functionCall("lower", identifier("function_name")),
                                    SortItem.Ordering.ASCENDING,
                                    SortItem.NullOrdering.UNDEFINED),
                            ascending("return_type"),
                            ascending("argument_types"),
                            ascending("function_type"))));
        }

        @Override
        protected Node visitShowExternalFunction(ShowExternalFunction node, Void context)
        {
            QualifiedObjectName functionName = qualifyObjectName(node.getName());
            Collection<? extends SqlFunction> functions = metadata.getFunctionAndTypeManager().getFunctions(session.getTransactionId(), functionName);
            if (node.getParameterTypes().isPresent()) {
                List<TypeSignature> parameterTypes = node.getParameterTypes().get().stream()
                        .map(TypeSignature::parseTypeSignature)
                        .collect(toImmutableList());
                functions = functions.stream()
                        .filter(function -> function.getSignature().getArgumentTypes().equals(parameterTypes))
                        .collect(toImmutableList());
            }
            if (functions.isEmpty()) {
                String types = node.getParameterTypes().map(parameterTypes -> format("(%s)", Joiner.on(", ").join(parameterTypes))).orElse("");
                throw new PrestoException(FUNCTION_NOT_FOUND, format("Function not found: %s%s", functionName, types));
            }

            ImmutableList.Builder<Expression> rows = ImmutableList.builder();
            for (SqlFunction function : functions) {
                if (!(function instanceof SqlInvokedFunction)) {
                    throw new PrestoException(GENERIC_USER_ERROR, "SHOW EXTERNAL FUNCTION is only supported for SQL functions");
                }

                SqlInvokedFunction sqlFunction = (SqlInvokedFunction) function;
                ImmutableList.Builder<FunctionProperty> functionPropertyBuilder = ImmutableList.builder();
                Map<String, String> propertyMap = sqlFunction.getFunctionProperties();
                for (Map.Entry<String, String> entry : propertyMap.entrySet()) {
                    functionPropertyBuilder.add(new FunctionProperty(new Identifier(entry.getKey()), new StringLiteral(entry.getValue())));
                }
                CreateFunction createFunction = new CreateFunction(
                        node.getName(),
                        false,
                        sqlFunction.getParameters().stream()
                                .map(parameter -> new SqlParameterDeclaration(new Identifier(parameter.getName()), parameter.getType().toString()))
                                .collect(toImmutableList()),
                        sqlFunction.getSignature().getReturnType().toString(),
                        Optional.of(sqlFunction.getDescription()),
                        new RoutineCharacteristics(
                                new RoutineCharacteristics.Language(sqlFunction.getRoutineCharacteristics().getLanguage().getLanguage()),
                                RoutineCharacteristics.Determinism.valueOf(sqlFunction.getRoutineCharacteristics().getDeterminism().name()),
                                RoutineCharacteristics.NullCallClause.valueOf(sqlFunction.getRoutineCharacteristics().getNullCallClause().name())),
                        sqlFunction.getBody().equals("EXTERNAL") ? new ExternalBodyReference() : sqlParser.createReturn(sqlFunction.getBody(), createParsingOptions(session, warningCollector)),
                        functionPropertyBuilder.build());
                rows.add(row(
                        new StringLiteral(formatSql(createFunction, Optional.empty())),
                        new StringLiteral(function.getSignature().getArgumentTypes().stream()
                                .map(TypeSignature::toString)
                                .collect(joining(", ")))));
            }

            Map<String, String> columns = ImmutableMap.<String, String>builder()
                    .put("external_function", "External Function")
                    .put("argument_types", "Argument Types")
                    .build();

            return simpleQuery(
                    selectAll(columns.entrySet().stream()
                            .map(entry -> aliasedName(entry.getKey(), entry.getValue()))
                            .collect(toImmutableList())),
                    aliased(new Values(rows.build()), "functions", ImmutableList.copyOf(columns.keySet())),
                    ordering(ascending("argument_types")));
        }

        private static String getFunctionType(SqlFunction function)
        {
            FunctionKind kind = function.getSignature().getKind();
            switch (kind) {
                case AGGREGATE:
                    return "aggregate";
                case WINDOW:
                    return "window";
                case SCALAR:
                    return "scalar";
                case EXTERNAL:
                    return "external";
            }
            throw new IllegalArgumentException("Unsupported function kind: " + kind);
        }

        @Override
        protected Node visitShowCache(ShowCache node, Void context)
        {
            if (!PropertyService.getBooleanProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED)) {
                throw new PrestoException(GENERIC_USER_ERROR, "Cache table feature is not enabled");
            }
            SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();
            Map<String, TableCacheInfo> cacheInfoByTableMap;
            if (node.getTableName() != null) {
                QualifiedObjectName fullObjectName = createQualifiedObjectName(session, node, node.getTableName());
                QualifiedName tableName = QualifiedName.of(fullObjectName.getCatalogName(), fullObjectName.getSchemaName(), fullObjectName.getObjectName());
                // Check if split cache has predicates for the requested table
                if (!splitCacheMap.cacheExists(tableName)) {
                    throw new SemanticException(MISSING_CACHE, node, "Cache for table '%s' does not exist", tableName.toString());
                }
                cacheInfoByTableMap = splitCacheMap.showCache(tableName.toString());
            }
            else {
                cacheInfoByTableMap = splitCacheMap.showCache();
            }
            ImmutableList.Builder<Expression> rows = ImmutableList.builder();
            //bogus row to support empty cache
            rows.add(row(new StringLiteral(""), new StringLiteral(""), new StringLiteral(""), FALSE_LITERAL));
            cacheInfoByTableMap.forEach((tableName, cacheInfo) -> {
                rows.add(row(
                        new StringLiteral(tableName.toString()),
                        new StringLiteral(cacheInfo.showPredicates()),
                        new StringLiteral(cacheInfo.showNodes()),
                        TRUE_LITERAL));
            });

            ImmutableList<Expression> expressions = rows.build();
            return simpleQuery(
                    selectList(
                            aliasedName("table_name", "Table"),
                            aliasedName("cached_predicates", "Cached Predicates"),
                            aliasedName("cached_nodes", "Cached Nodes")),
                    aliased(
                            new Values(expressions),
                            "Cache Result",
                            ImmutableList.of("table_name", "cached_predicates", "cached_nodes", "include")),
                    identifier("include"),
                    ordering(ascending("table_name")));
        }

        @Override
        protected Node visitShowIndex(ShowIndex node, Void context)
        {
            List<IndexRecord> indexRecords;
            try {
                String indexName = node.getIndexName();
                indexRecords = indexName == null ? heuristicIndexerManager.getAllIndexRecordsWithUsage() : heuristicIndexerManager.getIndexRecordWithUsage(indexName);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading index records, ", e);
            }

            // Access Control
            if (node.getIndexName() == null) {
                accessControl.checkCanShowIndex(session.getRequiredTransactionId(), session.getIdentity(), null);
            }
            else {
                for (IndexRecord record : indexRecords) {
                    QualifiedObjectName indexFullName = QualifiedObjectName.valueOf(record.qualifiedTable);
                    accessControl.checkCanShowIndex(session.getRequiredTransactionId(), session.getIdentity(), indexFullName);
                }
            }

            ImmutableList.Builder<Expression> rows = ImmutableList.builder();
            for (IndexRecord v : indexRecords) {
                String partitions = (v.partitions == null || v.partitions.isEmpty()) ? "all" : String.join(",", v.partitions);
                StringBuilder partitionsStrToDisplay = new StringBuilder();

                for (int i = 0; i < partitions.length(); i += COL_MAX_LENGTH) {
                    partitionsStrToDisplay.append(partitions, i, Math.min(i + COL_MAX_LENGTH, partitions.length()));
                    if (i + COL_MAX_LENGTH < partitions.length()) {
                        // have next line
                        partitionsStrToDisplay.append("\n");
                    }
                }

                QualifiedObjectName indexFullName = QualifiedObjectName.valueOf(v.qualifiedTable);

                String indexStatus;
                // if user runs SHOW INDEX while index creation is in-progress, show the duration in the status column
                if (v.isInProgressRecord()) {
                    long timeElapsed = System.currentTimeMillis() - v.lastModifiedTime;
                    Duration duration = Duration.succinctDuration(Double.valueOf(timeElapsed), TimeUnit.MILLISECONDS);
                    indexStatus = String.format("Creation in-progress for %s", duration.toString());
                }
                else {
                    Optional<TableHandle> tableHandle = metadata.getTableHandle(session, indexFullName);
                    if (!tableHandle.isPresent()) {
                        indexStatus = INDEX_TABLE_DELETED;
                    }
                    else {
                        LongSupplier lastModifiedTimeSupplier = metadata.getTableLastModifiedTimeSupplier(session, tableHandle.get());
                        long lastModifiedTime = lastModifiedTimeSupplier.getAsLong();
                        indexStatus = lastModifiedTime > v.lastModifiedTime ? INDEX_OUT_OF_SYNC : INDEX_OK;
                    }
                }

                rows.add(row(
                        new StringLiteral(v.name),
                        new StringLiteral(v.user),
                        new StringLiteral(v.qualifiedTable),
                        new StringLiteral(String.join(",", v.columns)),
                        new StringLiteral(v.indexType),
                        new StringLiteral(DataSize.succinctBytes(v.indexSize).toString()),
                        new StringLiteral(indexStatus),
                        new StringLiteral(partitionsStrToDisplay.toString()),
                        new StringLiteral(String.join(",", v.propertiesAsList)),
                        new StringLiteral(DataSize.succinctBytes(v.memoryUsage).toString()),
                        new StringLiteral(DataSize.succinctBytes(v.diskUsage).toString()), TRUE_LITERAL));
            }

            //bogus row to support empty index
            rows.add(row(new StringLiteral(""), new StringLiteral(""), new StringLiteral(""),
                    new StringLiteral(""), new StringLiteral(""), new StringLiteral(""),
                    new StringLiteral(""), new StringLiteral(""), new StringLiteral(""),
                    new StringLiteral(""), new StringLiteral(""), FALSE_LITERAL));

            ImmutableList<Expression> expressions = rows.build();
            return simpleQuery(
                    selectList(
                            aliasedName("index_name", "Index Name"),
                            aliasedName("user", "User"),
                            aliasedName("table_name", "Table Name"),
                            aliasedName("index_columns", "Index Columns"),
                            aliasedName("index_type", "Index Type"),
                            aliasedName("index_storage_size", "Index Size"),
                            aliasedName("index_status", "Index Status"),
                            aliasedName("partitions", "Partitions"),
                            aliasedName("index_props", "Index Properties"),
                            aliasedName("index_memoryUse", "Memory Usage (Coordinator Only)"),
                            aliasedName("index_diskUse", "Disk Usage (Coordinator Only)")),
                    aliased(
                            new Values(expressions),
                            "Index Result",
                            ImmutableList.of("index_name", "user", "table_name", "index_columns", "index_type", "index_storage_size", "index_status", "partitions", "index_props", "index_memoryUse", "index_diskUse", "include")),
                    identifier("include"),
                    ordering(ascending("index_name")));
        }

        @Override
        protected Node visitRefreshMetadataCache(RefreshMetadataCache node, Void context)
        {
            if (!node.getCatalog().isPresent() && !session.getCatalog().isPresent()) {
                throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
            }

            String catalog = node.getCatalog().map(Identifier::getValue).orElseGet(() -> session.getCatalog().get());
            metadata.refreshMetadataCache(session, Optional.of(catalog));
            return simpleQuery(selectList(ImmutableList.of()));
        }

        @Override
        protected Node visitShowSession(ShowSession node, Void context)
        {
            ImmutableList.Builder<Expression> rows = ImmutableList.builder();
            SortedMap<String, CatalogName> catalogNames = listCatalogs(session, metadata, accessControl);
            List<SessionPropertyValue> sessionProperties = metadata.getSessionPropertyManager().getAllSessionProperties(session, catalogNames);
            for (SessionPropertyValue sessionProperty : sessionProperties) {
                if (sessionProperty.isHidden()) {
                    continue;
                }

                String value = sessionProperty.getValue();
                String defaultValue = sessionProperty.getDefaultValue();
                rows.add(row(
                        new StringLiteral(sessionProperty.getFullyQualifiedName()),
                        new StringLiteral(nullToEmpty(value)),
                        new StringLiteral(nullToEmpty(defaultValue)),
                        new StringLiteral(sessionProperty.getType()),
                        new StringLiteral(sessionProperty.getDescription()),
                        TRUE_LITERAL));
            }

            // add bogus row so we can support empty sessions
            rows.add(row(new StringLiteral(""), new StringLiteral(""), new StringLiteral(""), new StringLiteral(""), new StringLiteral(""), FALSE_LITERAL));

            return simpleQuery(
                    selectList(
                            aliasedName("name", "Name"),
                            aliasedName("value", "Value"),
                            aliasedName("default", "Default"),
                            aliasedName("type", "Type"),
                            aliasedName("description", "Description")),
                    aliased(
                            new Values(rows.build()),
                            "session",
                            ImmutableList.of("name", "value", "default", "type", "description", "include")),
                    identifier("include"));
        }

        @Override
        protected Node visitShowViews(ShowViews showViews, Void context)
        {
            CatalogSchemaName schema = createCatalogSchemaName(session, showViews, showViews.getSchema());

            accessControl.checkCanShowTablesMetadata(session.getRequiredTransactionId(), session.getIdentity(), schema);

            if (!metadata.catalogExists(session, schema.getCatalogName())) {
                throw new SemanticException(MISSING_CATALOG, showViews, "Catalog '%s' does not exist", schema.getCatalogName());
            }

            if (!metadata.schemaExists(session, schema)) {
                throw new SemanticException(MISSING_SCHEMA, showViews, "Schema '%s' does not exist", schema.getSchemaName());
            }

            Expression predicate = equal(identifier("table_schema"), new StringLiteral(schema.getSchemaName()));
            Optional<String> likePattern = showViews.getLikePattern();
            if (likePattern.isPresent()) {
                /*
                 * Given that hive supports regex '*' to match string wildcard pattern
                 * we change '*' to '%' for hetu wildcard pattern matching
                 * */
                final char asterisk = '*';
                final char percent = '%';
                String likePatternStr = likePattern.get();
                if (likePattern.get().indexOf(asterisk) >= 0) {
                    likePatternStr = likePattern.get().replace(asterisk, percent);
                }
                Expression likePredicate = new LikePredicate(
                        identifier("table_name"),
                        new StringLiteral(likePatternStr),
                        showViews.getEscape().map(StringLiteral::new));
                predicate = logicalAnd(predicate, likePredicate);
            }

            return simpleQuery(
                    selectList(aliasedName("table_name", "Table")),
                    from(schema.getCatalogName(), TABLE_VIEWS),
                    predicate,
                    ordering(ascending("table_name")));
        }

        private Query parseView(String view, QualifiedObjectName name, Node node)
        {
            try {
                Statement statement = sqlParser.createStatement(view, createParsingOptions(session));
                return (Query) statement;
            }
            catch (ParsingException e) {
                throw new SemanticException(VIEW_PARSE_ERROR, node, "Failed parsing stored view '%s': %s", name, e.getMessage());
            }
        }

        private static Relation from(String catalog, SchemaTableName table)
        {
            return table(QualifiedName.of(catalog, table.getSchemaName(), table.getTableName()));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
